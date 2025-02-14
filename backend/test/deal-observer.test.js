import assert from 'node:assert'
import { after, before, beforeEach, describe, it } from 'node:test'
import { createPgPool, migrateWithPgClient } from '@filecoin-station/deal-observer-db'
import { fetchDealWithHighestActivatedEpoch, countStoredActiveDeals, loadDeals, storeActiveDeals, observeBuiltinActorEvents } from '../lib/deal-observer.js'
import { Value } from '@sinclair/typebox/value'
import { BlockEvent } from '../lib/rpc-service/data-types.js'
import { convertBlockEventToActiveDealDbEntry } from '../lib/utils.js'
import { PayloadRetrievabilityState } from '@filecoin-station/deal-observer-db/lib/types.js'
import { chainHeadTestData } from './test_data/chainHead.js'
import { rawActorEventTestData } from './test_data/rawActorEvent.js'
import { parse } from '@ipld/dag-json'

describe('deal-observer-backend', () => {
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
      payload_cid: undefined,
      payload_retrievability_state: PayloadRetrievabilityState.NotQueried,
      last_payload_retrieval_attempt: undefined
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
      payload_cid: undefined,
      payload_retrievability_state: PayloadRetrievabilityState.NotQueried,
      last_payload_retrieval_attempt: undefined
    }
    const event = Value.Parse(BlockEvent, { height: 1, event: eventData, emitter: 'f06' })
    const dbEntry = convertBlockEventToActiveDealDbEntry(event)
    await storeActiveDeals([dbEntry], pgPool)
    const expected = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    const actual = await fetchDealWithHighestActivatedEpoch(pgPool)
    assert.deepStrictEqual(expected, [actual])
  })

  it('check number of stored deals', async () => {
    const storeBlockEvent = async (eventData) => {
      const event = Value.Parse(BlockEvent, { height: 1, event: eventData, emitter: 'f06' })
      const dbEntry = convertBlockEventToActiveDealDbEntry(event)
      await storeActiveDeals([dbEntry], pgPool)
    }
    const data = {
      id: 1,
      provider: 2,
      client: 3,
      pieceCid: 'baga6ea4seaqc4z4432snwkztsadyx2rhoa6rx3wpfzu26365wvcwlb2wyhb5yfi',
      pieceSize: 4n,
      termStart: 5,
      termMin: 12340,
      termMax: 12340,
      sector: 6n
    }
    assert.strictEqual(await countStoredActiveDeals(pgPool), 0n)
    await storeBlockEvent(data)
    assert.strictEqual(await countStoredActiveDeals(pgPool), 1n)
    // Entries must be unique
    data.id = 2
    data.provider = 3
    await storeBlockEvent(data)
    assert.strictEqual(await countStoredActiveDeals(pgPool), 2n)
  })

  it('deal with duplicate events', async () => {
    const storeDeal = async (eventData) => {
      const event = Value.Parse(BlockEvent, { height: 1, event: eventData, emitter: 'f06' })
      const dbEntry = convertBlockEventToActiveDealDbEntry(event)
      await storeActiveDeals([dbEntry], pgPool)
    }
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
      payload_cid: undefined,
      payload_retrievability_state: PayloadRetrievabilityState.NotQueried,
      last_payload_retrieval_attempt: undefined
    }
    await storeDeal(eventData)
    let expected = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    assert.strictEqual(expected.length, 1)
    // If we only change the id, the unique constraint which does not include the id should prevent the insertion
    eventData.id = 2
    await storeDeal(eventData)
    expected = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    assert.strictEqual(expected.length, 1)
  })
})

describe('deal-observer-backend built in actor event observer', () => {
  let pgPool
  const makeRpcRequest = async (method, params) => {
    switch (method) {
      case 'Filecoin.ChainHead':
        return parse(JSON.stringify(chainHeadTestData))
      case 'Filecoin.GetActorEventsRaw':
        return parse(JSON.stringify(rawActorEventTestData)).filter(e => e.height >= params[0].fromHeight && e.height <= params[0].toHeight)
      default:
        console.error('Unknown method')
    }
  }
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
  it('stores all retrievable active deals if database is empty', async () => {
    await observeBuiltinActorEvents(pgPool, makeRpcRequest, 10, 0)
    const deals = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    assert.strictEqual(deals.length, 360)
  })

  it('correctly picks up from where the current storage is at', async () => {
    await observeBuiltinActorEvents(pgPool, makeRpcRequest, 11, 10)
    let deals = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    assert.strictEqual(deals.length, 25)
    const lastInsertedDeal = await fetchDealWithHighestActivatedEpoch(pgPool)
    assert.strictEqual(lastInsertedDeal.activated_at_epoch, 4622129)

    // The deal observer function should pick up from the current storage
    await observeBuiltinActorEvents(pgPool, makeRpcRequest, 100, 0)
    deals = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    assert.strictEqual(deals.length, 360)
  })

  it('correctly applies the max past epoch and finality epoch parameters', async () => {
    await observeBuiltinActorEvents(pgPool, makeRpcRequest, 11, 12)
    let deals = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    // No deals should be stored because the finality epoch is larger than the maximum past epoch parameter
    assert.strictEqual(deals.length, 0)

    await observeBuiltinActorEvents(pgPool, makeRpcRequest, 11, 10)
    deals = await loadDeals(pgPool, 'SELECT * FROM active_deals')
    // There should be only one distinct block height in the database
    assert.strictEqual((new Set(deals.map(deal => deal.activated_at_epoch))).size, 1)
  })
})
