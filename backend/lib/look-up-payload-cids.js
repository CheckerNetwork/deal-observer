import { loadDeals } from './deal-observer.js'
import * as util from 'node:util'
import { getMinerPeerId } from './rpc-service/service.js'
import { PayloadRetrievabilityState } from '@filecoin-station/deal-observer-db/lib/types.js'

/** @import {Queryable} from '@filecoin-station/deal-observer-db' */
/** @import { Static } from '@sinclair/typebox' */
/** @import { ActiveDealDbEntry, PayloadRetrievabilityStateType } from '@filecoin-station/deal-observer-db/lib/types.js' */

const THREE_DAYS_IN_MILLISECONDS = 1000 * 60 * 60 * 24 * 3

/**
 *
 * @param {function} makeRpcRequest
 * @param {function} getDealPayloadCid
 * @param {Queryable} pgPool
 * @param {number} maxDeals
 * @returns {Promise<number>}
 */
export const lookUpPayloadCids = async (makeRpcRequest, getDealPayloadCid, pgPool, maxDeals, now = Date.now()) => {
  let missingPayloadCidsResolved = 0
  for (const deal of await fetchDealsWithUnresolvedPayloadCid(pgPool, maxDeals, new Date(now - THREE_DAYS_IN_MILLISECONDS))) {
    const minerPeerId = await getMinerPeerId(deal.miner_id, makeRpcRequest)
    deal.payload_cid = await getDealPayloadCid(minerPeerId, deal.piece_cid)
    if (!deal.payload_cid) {
      if (deal.last_payload_retrieval_attempt) {
        deal.payload_retrievability_state = PayloadRetrievabilityState.TerminallyUnretrievable
      } else {
        deal.payload_retrievability_state = PayloadRetrievabilityState.Unresolved
      }
    } else {
      missingPayloadCidsResolved++
      deal.payload_retrievability_state = PayloadRetrievabilityState.Resolved
    }
    deal.last_payload_retrieval_attempt = new Date(now)
    await updatePayloadCidInActiveDeal(pgPool, deal, deal.payload_retrievability_state, deal.last_payload_retrieval_attempt, deal.payload_cid)
  }
  return missingPayloadCidsResolved
}

/**
   * @param {Queryable} pgPool
   * @param {number} maxDeals
   * @param {Date} now
   * @returns {Promise<Array<Static< typeof ActiveDealDbEntry>>>}
   */
export async function fetchDealsWithUnresolvedPayloadCid (pgPool, maxDeals, now) {
  const query = "SELECT * FROM active_deals WHERE payload_cid IS NULL AND (payload_retrievability_state = 'PAYLOAD_CID_NOT_QUERIED_YET' OR payload_retrievability_state = 'PAYLOAD_CID_UNRESOLVED') AND (last_payload_retrieval_attempt IS NULL OR last_payload_retrieval_attempt < $1) ORDER BY activated_at_epoch ASC LIMIT $2"
  return await loadDeals(pgPool, query, [now, maxDeals])
}

export async function countStoredActiveDealsWithUnresolvedPayloadCid (pgPool) {
  const query = 'SELECT COUNT(*) FROM active_deals WHERE payload_cid IS NULL'
  const result = await pgPool.query(query)
  return result.rows[0].count
}

/**
  * @param {Queryable} pgPool
  * @returns {Promise<Array<Static<typeof ActiveDealDbEntry>>>}
  */
export async function countRevertedActiveDeals (pgPool) {
  const query = 'SELECT COUNT(*) FROM active_deals WHERE reverted = TRUE'
  const result = await pgPool.query(query)
  return result.rows[0].count
}

/**
 * @param {Queryable} pgPool
 * @param {Static<typeof ActiveDealDbEntry>} deal
 * @param {Static< typeof PayloadRetrievabilityStateType>} newPayloadRetrievalState
 * @param {Date} lastRetrievalAttemptTimestamp
 * @param {string} newPayloadCid
 * @returns { Promise<void>}
 */
async function updatePayloadCidInActiveDeal (pgPool, deal, newPayloadRetrievalState, lastRetrievalAttemptTimestamp, newPayloadCid) {
  const updateQuery = `
    UPDATE active_deals
    SET payload_cid = $1, payload_retrievability_state = $2, last_payload_retrieval_attempt = $3
    WHERE activated_at_epoch = $4 AND miner_id = $5 AND client_id = $6 AND piece_cid = $7 AND piece_size = $8 AND term_start_epoch = $9 AND term_min = $10 AND term_max = $11 AND sector_id = $12
  `
  try {
    await pgPool.query(updateQuery, [
      newPayloadCid,
      newPayloadRetrievalState,
      lastRetrievalAttemptTimestamp,
      deal.activated_at_epoch,
      deal.miner_id,
      deal.client_id,
      deal.piece_cid,
      deal.piece_size,
      deal.term_start_epoch,
      deal.term_min,
      deal.term_max,
      deal.sector_id
    ])
  } catch (error) {
    throw Error(util.format('Error updating payload of deal: ', deal), { cause: error })
  }
}
