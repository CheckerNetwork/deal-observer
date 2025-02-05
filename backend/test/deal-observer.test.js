import assert from 'node:assert'
import { after, before, beforeEach, describe, it } from 'node:test'
import { createPgPool, migrateWithPgClient } from '@filecoin-station/deal-observer-db'
import { fetchDealWithHighestActivatedEpoch, countStoredActiveDeals, loadDeals, storeActiveDeals } from '../lib/deal-observer.js'
import { Value } from '@sinclair/typebox/value'
import { BlockEvent, ClaimEvent } from '../lib/rpc-service/data-types.js'
import { convertBlockEventToActiveDealDbEntry } from '../lib/utils.js'
/** @import {PgPool} from '@filecoin-station/deal-observer-db' */
/** @import { Static } from '@sinclair/typebox' */

describe('deal-observer-backend', () => {
  /**
   * @type {PgPool}
   */
  let pgPool
  before(async () => {
    pgPool = await createPgPool()
    await migrateWithPgClient(pgPool)
  })

  after(async () => {
    await pgPool.end()
  })

  beforeEach(async () => {
    await pgPool.query('DELETE FROM active_deals')
  })

  it('adds new FIL+ deals from built-in actor events to storage', async () => {
    const eventData = {
      id: 1,
      provider: 2,
      client: 3,
      pieceCid: 'baga6ea4seaqc4z4432snwkztsadyx2rhoa6rx3wpfzu26365wvcwlb2wyhb5yfi',
      pieceSize: 4n,
      termStart: 5,
      termMin: 12340,
      termMax: 12340,
      sector: 6n,
      payload_cid: undefined
    }
    const event = Value.Parse(BlockEvent, { height: 1, event: eventData, emitter: 'f06' })
    const dbEntry = convertBlockEventToActiveDealDbEntry(event)
    await storeActiveDeals([dbEntry], pgPool)
    const actualData = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    const expectedData = {
      activated_at_epoch: event.height,
      miner_id: eventData.provider,
      client_id: eventData.client,
      piece_cid: eventData.pieceCid,
      piece_size: eventData.pieceSize,
      term_start_epoch: eventData.termStart,
      term_min: eventData.termMin,
      term_max: eventData.termMax,
      sector_id: eventData.sector,
      payload_cid: undefined
    }
    assert.deepStrictEqual(actualData, [expectedData])
  })
  it('check retrieval of last stored deal', async () => {
    const eventData = {
      id: 1,
      provider: 2,
      client: 3,
      pieceCid: 'baga6ea4seaqc4z4432snwkztsadyx2rhoa6rx3wpfzu26365wvcwlb2wyhb5yfi',
      pieceSize: 4n,
      termStart: 5,
      termMin: 12340,
      termMax: 12340,
      sector: 6n,
      payload_cid: undefined
    }
    const event = Value.Parse(BlockEvent, { height: 1, event: eventData, emitter: 'f06' })
    const dbEntry = convertBlockEventToActiveDealDbEntry(event)
    await storeActiveDeals([dbEntry], pgPool)
    const expected = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    const actual = await fetchDealWithHighestActivatedEpoch(pgPool)
    assert.deepStrictEqual(expected, [actual])
  })

  it('check number of stored deals', async () => {
    /**
     * @param {Static<typeof ClaimEvent>} eventData
     */
    const storeBlockEvent = async (eventData) => {
      const event = Value.Parse(BlockEvent, { height: 1, event: eventData, emitter: 'f06' })
      const dbEntry = convertBlockEventToActiveDealDbEntry(event)
      await storeActiveDeals([dbEntry], pgPool)
    }
    const data = Value.Parse(ClaimEvent, {
      id: 1,
      provider: 2,
      client: 3,
      pieceCid: 'baga6ea4seaqc4z4432snwkztsadyx2rhoa6rx3wpfzu26365wvcwlb2wyhb5yfi',
      pieceSize: 4n,
      termStart: 5,
      termMin: 12340,
      termMax: 12340,
      sector: 6n
    })
    assert.strictEqual(await countStoredActiveDeals(pgPool), 0n)
    await storeBlockEvent(data)
    assert.strictEqual(await countStoredActiveDeals(pgPool), 1n)
    // Entries must be unique
    data.id = 2
    data.provider = 3
    await storeBlockEvent(data)
    assert.strictEqual(await countStoredActiveDeals(pgPool), 2n)
  })
})
