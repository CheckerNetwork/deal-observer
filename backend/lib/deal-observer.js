/** @import {Queryable} from '@filecoin-station/deal-observer-db' */
/** @import { Static } from '@sinclair/typebox' */

import { getActorEvents, getActorEventsFilter } from './rpc-service/service.js'
import { ActiveDealDbEntry } from '@filecoin-station/deal-observer-db/lib/types.js'
import { Value } from '@sinclair/typebox/value'
import { convertBlockEventToActiveDealDbEntry } from './utils.js'

/**
 * @param {number} blockHeight
 * @param {Queryable} pgPool
 * @param {(method:string,params:any[]) => Promise<any>} makeRpcRequest
 * @returns {Promise<void>}
 */
export async function observeBuiltinActorEvents (blockHeight, pgPool, makeRpcRequest) {
  const eventType = 'claim'
  const blockEvents = await getActorEvents(getActorEventsFilter(blockHeight, eventType), makeRpcRequest)
  console.log(`Observed ${blockEvents.length} ${eventType} events in block ${blockHeight}`)
  await storeActiveDeals(blockEvents.map((event) => convertBlockEventToActiveDealDbEntry(event)), pgPool)
}

/**
 * @param {Queryable} pgPool
 * @returns {Promise<Static<typeof ActiveDealDbEntry> | null>}
 */
export async function fetchDealWithHighestActivatedEpoch (pgPool) {
  const query = 'SELECT * FROM active_deals ORDER BY activated_at_epoch DESC LIMIT 1'
  const result = await loadDeals(pgPool, query)
  return result.length > 0 ? result[0] : null
}

/**
 * @param {Queryable} pgPool
 * @returns {Promise<number>}
 * */
export async function countStoredActiveDeals (pgPool) {
  const query = 'SELECT COUNT(*) FROM active_deals'
  const result = await pgPool.query(query)
  return result.rows[0].count
}

/**
 * @param {Static<typeof ActiveDealDbEntry >[]} activeDeals
 * @param {Queryable} pgPool
 * @returns {Promise<void>}
 * */
export async function storeActiveDeals (activeDeals, pgPool) {
  if (activeDeals.length === 0) {
    return
  }
  try {
    // Insert deals in a batch
    const insertQuery = `
        INSERT INTO active_deals (
          activated_at_epoch,
          miner_id,
          client_id,
          piece_cid,
          piece_size,
          term_start_epoch,
          term_min,
          term_max,
          sector_id
        )
        VALUES (
          unnest($1::int[]),
          unnest($2::int[]), 
          unnest($3::int[]), 
          unnest($4::text[]), 
          unnest($5::bigint[]), 
          unnest($6::int[]), 
          unnest($7::int[]), 
          unnest($8::int[]), 
          unnest($9::bigint[])
        )
      `
    await pgPool.query(insertQuery, [
      activeDeals.map(deal => deal.activated_at_epoch),
      activeDeals.map(deal => deal.miner_id),
      activeDeals.map(deal => deal.client_id),
      activeDeals.map(deal => deal.piece_cid),
      activeDeals.map(deal => deal.piece_size),
      activeDeals.map(deal => deal.term_start_epoch),
      activeDeals.map(deal => deal.term_min),
      activeDeals.map(deal => deal.term_max),
      activeDeals.map(deal => deal.sector_id)
    ])
  } catch (error) {
    // If any error occurs, roll back the transaction
    throw Error('Error inserting deals', { cause: error })
  }
}

/**
   * @param {Queryable} pgPool
   * @param {string} query
   * @param {Array<number | string>} args
   * @returns {Promise<Array<Static <typeof ActiveDealDbEntry>>>}
   */
export async function loadDeals (pgPool, query, args = []) {
  const result = (await pgPool.query(query, args)).rows.map((/** @type {any} */ deal) => {
    // SQL used null, typebox needs undefined for null values
    Object.keys(deal).forEach(key => {
      if (deal[key] === null) {
        deal[key] = undefined
      }
    })
    return Value.Parse(ActiveDealDbEntry, deal)
  }
  )
  return result
}
