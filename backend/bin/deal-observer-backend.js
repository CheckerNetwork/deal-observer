/** @import {PgPool} from '@filecoin-station/deal-observer-db' */
import assert from 'node:assert'
import { createPgPool } from '@filecoin-station/deal-observer-db'
import * as Sentry from '@sentry/node'
import timers from 'node:timers/promises'
import slug from 'slug'
import '../lib/instrument.js'
import { createInflux } from '../lib/telemetry.js'
import { getChainHead, rpcRequest } from '../lib/rpc-service/service.js'
import { fetchDealWithHighestActivatedEpoch, countStoredActiveDeals, observeBuiltinActorEvents } from '../lib/deal-observer.js'
import { indexPieces } from '../lib/piece-indexer.js'
import { findAndSubmitUnsubmittedDeals, submitDealsToSparkApi } from '../lib/spark-api-submit-deals.js'
import { getDealPayloadCid } from '../lib/piece-indexer-service.js'
/** @import {Queryable} from '@filecoin-station/deal-observer-db' */

const {
  INFLUXDB_TOKEN,
  SPARK_API_BASE_URL,
  SPARK_API_TOKEN,
  SPARK_API_SUBMIT_DEALS_BATCH_SIZE = 100
} = process.env

if (!INFLUXDB_TOKEN) {
  console.error('INFLUXDB_TOKEN not provided. Telemetry will not be recorded.')
}
assert(SPARK_API_BASE_URL, 'SPARK_API_BASE_URL required')
assert(SPARK_API_TOKEN, 'SPARK_API_TOKEN required')

const LOOP_INTERVAL = 10 * 1000

// Filecoin will need some epochs to reach finality.
// We do not want to fetch deals that are newer than the current chain head - 940 epochs.
const finalityEpochs = 940
// The free tier of the glif rpc endpoint only allows us to go back 2000 blocks.
const maxPastEpochs = 1999
assert(finalityEpochs <= maxPastEpochs)

const pgPool = await createPgPool()
const { recordTelemetry } = createInflux(INFLUXDB_TOKEN)

/**
 * @param {(method:string,params:any[]) => Promise<any>} makeRpcRequest
 * @param {Queryable} pgPool
 */
const observeActorEventsLoop = async (makeRpcRequest, pgPool) => {
  const LOOP_NAME = 'Observe actor events'
  while (true) {
    const start = Date.now()
    try {
      const currentChainHead = await getChainHead(makeRpcRequest)
      const lastInsertedDeal = await fetchDealWithHighestActivatedEpoch(pgPool)
      const startEpoch = Math.max(
        currentChainHead.Height - maxPastEpochs,
        lastInsertedDeal ? (lastInsertedDeal.activated_at_epoch ?? -1) + 1 : 0
      )
      const endEpoch = currentChainHead.Height - finalityEpochs

      for (let epoch = startEpoch; epoch <= endEpoch; epoch++) {
        await observeBuiltinActorEvents(epoch, pgPool, makeRpcRequest)
      }
      const newLastInsertedDeal = await fetchDealWithHighestActivatedEpoch(pgPool)
      const numberOfStoredDeals = await countStoredActiveDeals(pgPool)
      if (INFLUXDB_TOKEN) {
        recordTelemetry('observed_deals_stats', point => {
          point.intField('last_searched_epoch', newLastInsertedDeal?.activated_at_epoch || 0)
          point.intField('number_of_stored_active_deals', numberOfStoredDeals)
        })
      }
    } catch (e) {
      console.error(e)
      Sentry.captureException(e)
    }
    const dt = Date.now() - start
    console.log(`Loop "${LOOP_NAME}" took ${dt}ms`)

    if (INFLUXDB_TOKEN) {
      recordTelemetry(`loop_${slug(LOOP_NAME, '_')}`, point => {
        point.intField('interval_ms', LOOP_INTERVAL)
        point.intField('duration_ms', dt)
      })
    }
    if (dt < LOOP_INTERVAL) {
      await timers.setTimeout(LOOP_INTERVAL - dt)
    }
  }
}

/**
 * Periodically fetches unsubmitted deals from the database and submits them to Spark API.
 *
 * @param {PgPool} pgPool
 * @param {object} args
 * @param {string} args.sparkApiBaseUrl
 * @param {string} args.sparkApiToken
 * @param {number} args.sparkApiSubmitDealsBatchSize
 */
const sparkApiSubmitDealsLoop = async (pgPool, { sparkApiBaseUrl, sparkApiToken, sparkApiSubmitDealsBatchSize }) => {
  const LOOP_NAME = 'Submit deals to spark-api'
  while (true) {
    const start = Date.now()
    try {
      const { submitted, ingested, skipped } = await findAndSubmitUnsubmittedDeals(
        pgPool,
        sparkApiSubmitDealsBatchSize,
        deals => submitDealsToSparkApi(sparkApiBaseUrl, sparkApiToken, deals)
      )

      if (INFLUXDB_TOKEN) {
        recordTelemetry('submitted_deals_stats', point => {
          point.intField('submitted_deals', submitted)
          point.intField('ingested_deals', ingested)
          point.intField('skipped_deals', skipped)
        })
      }
    } catch (e) {
      console.error(e)
      Sentry.captureException(e)
    }
    const dt = Date.now() - start
    console.log(`Loop "${LOOP_NAME}" took ${dt}ms`)

    if (INFLUXDB_TOKEN) {
      recordTelemetry(`loop_${slug(LOOP_NAME, '_')}`, point => {
        point.intField('interval_ms', LOOP_INTERVAL)
        point.intField('duration_ms', dt)
      })
    }
    if (dt < LOOP_INTERVAL) {
      await timers.setTimeout(LOOP_INTERVAL - dt)
    }
  }
}

/**
 * @param {(method:string,params:object) => object} makeRpcRequest
 * @param {(providerId:string,pieceCid:string) => Promise<string|null>} getDealPayloadCid
 * @param {*} pgPool
 */
export const pieceIndexerLoop = async (makeRpcRequest, getDealPayloadCid, pgPool) => {
  const LOOP_NAME = 'Piece Indexer'
  while (true) {
    const start = Date.now()
    // Maximum number of deals to index in one loop iteration
    const maxDeals = 1000
    try {
      await indexPieces(makeRpcRequest, getDealPayloadCid, pgPool, maxDeals)
    } catch (e) {
      console.error(e)
      Sentry.captureException(e)
    }
    const dt = Date.now() - start
    console.log(`Loop "${LOOP_NAME}" took ${dt}ms`)

    // For local monitoring and debugging, we can omit sending data to InfluxDB
    if (INFLUXDB_TOKEN) {
      recordTelemetry(`loop_${slug(LOOP_NAME, '_')}`, point => {
        point.intField('interval_ms', LOOP_INTERVAL)
        point.intField('duration_ms', dt)
      })
    }
    if (dt < LOOP_INTERVAL) {
      await timers.setTimeout(LOOP_INTERVAL - dt)
    }
  }
}

await Promise.all([
  pieceIndexerLoop(rpcRequest, getDealPayloadCid, pgPool),
  observeActorEventsLoop(rpcRequest, pgPool),
  sparkApiSubmitDealsLoop(pgPool, {
    sparkApiBaseUrl: SPARK_API_BASE_URL,
    sparkApiToken: SPARK_API_TOKEN,
    sparkApiSubmitDealsBatchSize: Number(SPARK_API_SUBMIT_DEALS_BATCH_SIZE)
  })
])
