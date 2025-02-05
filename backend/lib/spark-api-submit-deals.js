/** @import {PgPool, Queryable} from '@filecoin-station/deal-observer-db' */
import Cursor from 'pg-cursor'
import * as Sentry from '@sentry/node'

/**
 * Finds deals that haven't been submitted to the Spark API yet and submits them.
 *
 * @param {PgPool} pgPool
 * @param {number} batchSize
 * @param {(eligibleDeals: Array<any>) => Promise<{ingested: number; skipped: number}>} submitDeals
 * @returns {Promise<{submitted: number; ingested: number; skipped: number;}>} Number of deals submitted, ingested and skipped
 */
export const findAndSubmitUnsubmittedDeals = async (pgPool, batchSize, submitDeals) => {
  const result = { submitted: 0, ingested: 0, skipped: 0 }
  for await (const deals of findUnsubmittedDeals(pgPool, batchSize)) {
    console.debug(`Found ${deals.length} unsubmitted deals`)
    try {
      const { ingested, skipped } = await submitDeals(deals)
      console.log(`Successfully submitted ${deals.length} deals. ${ingested} deals were added, ${skipped} were skipped.`)
      await markDealsAsSubmitted(pgPool, deals)
      result.submitted += deals.length
      result.ingested += ingested
      result.skipped += skipped
    } catch (e) {
      console.error('Failed to submit deals:', e)
      Sentry.captureException(e)
    }
  }

  return result
}

/**
 * Finds deals that haven't been submitted to spark api.
 * For deals to be submitted they must
 * - have not been submitted yet
 * - were created more than 2 days ago
 *     Explanation by @pyropy:
 *     > This due that whole implication of overriding deals ingested by
 *     > fil-deal-ingester. Observed deals might lack Payload CID (it's set to
 *     > null) and that might override Payload CID that's already in the
 *     > database)
 * - have payload cid
 * - have not yet expired
 *
 * @param {PgPool} pgPool
 * @param {number} batchSize
 * @returns {AsyncGenerator<Array<any>>}
 */
const findUnsubmittedDeals = async function * (pgPool, batchSize) {
  const client = await pgPool.connect()
  const cursor = client.query(new Cursor(`
    WITH two_days_ago AS (
        SELECT (NOW() - INTERVAL '2 days')::TIMESTAMP AS ts
    )
    SELECT
        miner_id,
        client_id,
        piece_cid,
        piece_size,
        payload_cid,
        epoch_to_timestamp(term_start_epoch + term_min) AS expires_at
    FROM
        active_deals
    WHERE
        submitted_at IS NULL
        AND payload_cid IS NOT NULL
        AND activated_at_epoch < timestamp_to_epoch((SELECT ts FROM two_days_ago))
        AND epoch_to_timestamp(term_start_epoch + term_min) > NOW()`
  ))

  while (true) {
    const rows = await cursor.read(batchSize)
    if (rows.length === 0) break
    yield rows
  }

  client.release()
}

/**
 * Mark deals as submitted.
 *
 * @param {Queryable} pgPool
 * @param {Array<any>} eligibleDeals
 */
const markDealsAsSubmitted = async (pgPool, eligibleDeals) => {
  await pgPool.query(`
    UPDATE active_deals ad
    SET submitted_at = NOW()
    FROM (
      SELECT
        unnest($1::INT[]) AS miner_id,
        unnest($2::INT[]) AS client_id,
        unnest($3::TEXT[]) AS piece_cid,
        unnest($4::BIGINT[]) AS piece_size
    ) AS t
    WHERE ad.miner_id = t.miner_id
      AND ad.client_id = t.client_id
      AND ad.piece_cid = t.piece_cid
      AND ad.piece_size = t.piece_size
  `, [
    eligibleDeals.map(deal => deal.miner_id),
    eligibleDeals.map(deal => deal.client_id),
    eligibleDeals.map(deal => deal.piece_cid),
    eligibleDeals.map(deal => deal.piece_size)
  ])
}

/**
 * Submits deals to a spark api.
 *
 * @param {string} sparkApiBaseURL
 * @param {string} sparkApiToken
 * @param {Array<any>} deals
 * @returns {Promise<{ingested: number; skipped: number}>}
 */
export const submitDealsToSparkApi = async (sparkApiBaseURL, sparkApiToken, deals) => {
  console.debug(`Submitting ${deals.length} deals to Spark API`)
  const response = await fetch(`${sparkApiBaseURL}/eligible-deals-batch`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${sparkApiToken}`
    },
    body: JSON.stringify(deals.map(deal => ({
      minerId: `f0${deal.miner_id}`,
      clientId: `f0${deal.client_id}`,
      pieceCid: deal.piece_cid,
      pieceSize: deal.piece_size.toString(),
      payloadCid: deal.payload_cid,
      expiresAt: deal.expires_at
    })))
  })

  if (!response.ok) {
    let msg = `Failed to submit deals (status ${response.status}): ${await response.text().catch(() => null)}`
    if (response.status === 400) {
      const stringified = JSON.stringify(
        deals[0],
        (_, v) => typeof v === 'bigint' ? `<bigint> ${v.toString()}` : v
      )
      msg += `\ndeals[0]: ${stringified}}`
    }
    throw new Error(msg)
  }

  return /** @type {{ingested: number; skipped: number}} */ (await response.json())
}
