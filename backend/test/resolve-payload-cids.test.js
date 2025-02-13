import { createPgPool, migrateWithPgClient } from '@filecoin-station/deal-observer-db'
import { before, beforeEach, it, describe, after } from 'node:test'
import { rawActorEventTestData } from './test_data/rawActorEvent.js'
import { chainHeadTestData } from './test_data/chainHead.js'
import { parse } from '@ipld/dag-json'
import { loadDeals, fetchAndStoreActiveDeals, storeActiveDeals } from '../lib/deal-observer.js'
import assert from 'assert'
import { minerPeerIds } from './test_data/minerInfo.js'
import { payloadCIDs } from './test_data/payloadCIDs.js'
import { Value } from '@sinclair/typebox/value'
import { ActiveDealDbEntry, PayloadRetrievabilityState } from '@filecoin-station/deal-observer-db/lib/types.js'
import { countStoredActiveDealsWithUnresolvedPayloadCid, resolvePayloadCids } from '../lib/resolve-payload-cids.js'

describe('deal-observer-backend resolve payload CIDs', () => {
  const makeRpcRequest = async (method, params) => {
    switch (method) {
      case 'Filecoin.ChainHead':
        return parse(JSON.stringify(chainHeadTestData))
      case 'Filecoin.GetActorEventsRaw':
        return parse(JSON.stringify(rawActorEventTestData)).filter(e => e.height >= params[0].fromHeight && e.height <= params[0].toHeight)
      case 'Filecoin.StateMinerInfo':
        return minerPeerIds.get(params[0])
      default:
        console.error('Unknown method')
    }
  }
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
    const startEpoch = 4622129
    for (let blockHeight = startEpoch; blockHeight < startEpoch + 10; blockHeight++) {
      await fetchAndStoreActiveDeals(blockHeight, pgPool, makeRpcRequest)
    }
    assert.strictEqual(
      (await pgPool.query('SELECT * FROM active_deals')).rows.length,
      336
    )
  })

  it('piece indexer loop function fetches deals where there exists no payload yet and updates the database entry', async (t) => {
    const resolvePayloadCidCalls = []
    const resolvePayloadCid = async (providerId, pieceCid) => {
      resolvePayloadCidCalls.push({ providerId, pieceCid })
      const payloadCid = payloadCIDs.get(JSON.stringify({ minerId: providerId, pieceCid }))
      return payloadCid?.payloadCid
    }

    assert.strictEqual(
      (await pgPool.query('SELECT * FROM active_deals WHERE payload_cid IS NULL')).rows.length,
      336
    )
    await resolvePayloadCids(makeRpcRequest, resolvePayloadCid, pgPool, 10000)
    assert.strictEqual(resolvePayloadCidCalls.length, 336)
    assert.strictEqual(
      (await pgPool.query('SELECT * FROM active_deals WHERE payload_cid IS NULL')).rows.length,
      85 // Not all deals have a payload CID in the test data
    )
  })

  it('piece indexer count number of unresolved payload CIDs', async () => {
    let unresolvedPayloadCids = await countStoredActiveDealsWithUnresolvedPayloadCid(pgPool)
    assert.strictEqual(unresolvedPayloadCids, 336n)
    const resolvePayloadCidCalls = []
    const resolvePayloadCid = async (providerId, pieceCid) => {
      resolvePayloadCidCalls.push({ providerId, pieceCid })
      const payloadCid = payloadCIDs.get(JSON.stringify({ minerId: providerId, pieceCid }))
      return payloadCid?.payloadCid
    }

    await resolvePayloadCids(makeRpcRequest, resolvePayloadCid, pgPool, 10000)
    unresolvedPayloadCids = await countStoredActiveDealsWithUnresolvedPayloadCid(pgPool)
    assert.strictEqual(unresolvedPayloadCids, 85n)
  })
})

describe('deal-observer-backend piece indexer payload retrieval', () => {
  let pgPool
  const payloadCid = 'PAYLOAD_CID'
  const minerPeerId = 'MINER_PEER_ID'
  const pieceCid = 'PIECE_CID'
  const now = Date.now()
  const fetchMinerId = async () => {
    return { PeerId: minerPeerId }
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
  it('piece indexer does not retry to fetch unresolved payloads if the last retrieval was too recent', async (t) => {
    const returnPayload = false
    let payloadsCalled = 0
    const resolvePayloadCid = async () => {
      payloadsCalled++
      return returnPayload ? payloadCid : null
    }

    const deal = Value.Parse(ActiveDealDbEntry, {
      miner_id: 1,
      piece_cid: pieceCid,
      client_id: 1,
      activated_at_epoch: 1,
      piece_size: 1000,
      term_start_epoch: 1,
      term_min: 1,
      term_max: 1,
      sector_id: 1,
      payload_cid: undefined,
      payload_retrievability_state: PayloadRetrievabilityState.NotQueried,
      last_payload_retrieval_attempt: undefined
    })

    await storeActiveDeals([deal], pgPool)
    assert.deepStrictEqual((await loadDeals(pgPool, 'SELECT * FROM active_deals'))[0], deal)
    // The payload is unresolvable and the last retrieval timestamp should be updated
    await resolvePayloadCids(fetchMinerId, resolvePayloadCid, pgPool, 10000, now)
    // The timestamp on when the last retrieval of the payload was, was not yet set, so the piece indexer will try to fetch the payload
    assert.strictEqual(payloadsCalled, 1)
    deal.last_payload_retrieval_attempt = new Date(now)
    deal.payload_retrievability_state = PayloadRetrievabilityState.Unresolved
    assert.deepStrictEqual((await loadDeals(pgPool, 'SELECT * FROM active_deals'))[0], deal)
    // If we retry now without changing the field last_payload_retrieval_attempt the function for calling payload should not be called
    await resolvePayloadCids(fetchMinerId, resolvePayloadCid, pgPool, 10000, now)
    assert.strictEqual(payloadsCalled, 1)
  })

  it('piece indexer sets the payload to be unresolvable if the second attempt fails', async (t) => {
    const returnPayload = false
    let payloadsCalled = 0
    const resolvePayloadCid = async () => {
      payloadsCalled++
      return returnPayload ? payloadCid : null
    }
    // If we set the last_payload_retrieval_attempt to a value more than three days ago the piece indexer should try again to fetch the payload CID
    const deal = Value.Parse(ActiveDealDbEntry, {
      miner_id: 1,
      piece_cid: pieceCid,
      client_id: 1,
      activated_at_epoch: 1,
      piece_size: 1000,
      term_start_epoch: 1,
      term_min: 1,
      term_max: 1,
      sector_id: 1,
      payload_cid: undefined,
      payload_retrievability_state: PayloadRetrievabilityState.NotQueried,
      last_payload_retrieval_attempt: new Date(now - 1000 * 60 * 60 * 24 * 4)
    })

    await storeActiveDeals([deal], pgPool)
    await resolvePayloadCids(fetchMinerId, resolvePayloadCid, pgPool, 10000, now)
    assert.strictEqual(payloadsCalled, 1)
    // This is the second attempt that failed to fetch the payload CID so the deal should be marked as unresolvable
    deal.payload_retrievability_state = PayloadRetrievabilityState.TerminallyUnretrievable
    deal.last_payload_retrieval_attempt = new Date(now)
    assert.deepStrictEqual((await loadDeals(pgPool, 'SELECT * FROM active_deals'))[0], deal)
    // Now the piece indexer should no longer call the payload request for this deal
    await resolvePayloadCids(fetchMinerId, resolvePayloadCid, pgPool, 10000, now)
    assert.strictEqual(payloadsCalled, 1)
  })

  it('piece indexer correctly udpates the payloads if the retry succeeeds', async (t) => {
    const returnPayload = true
    let payloadsCalled = 0
    const resolvePayloadCid = async () => {
      payloadsCalled++
      return returnPayload ? payloadCid : null
    }
    // If we set the last_payload_retrieval_attempt to a value more than three days ago the piece indexer should try again to fetch the payload CID
    const deal = Value.Parse(ActiveDealDbEntry, {
      miner_id: 1,
      piece_cid: pieceCid,
      client_id: 1,
      activated_at_epoch: 1,
      piece_size: 1000,
      term_start_epoch: 1,
      term_min: 1,
      term_max: 1,
      sector_id: 1,
      payload_cid: undefined,
      payload_retrievability_state: PayloadRetrievabilityState.NotQueried,
      last_payload_retrieval_attempt: new Date(now - 1000 * 60 * 60 * 24 * 4)
    })

    await storeActiveDeals([deal], pgPool)
    await resolvePayloadCids(fetchMinerId, resolvePayloadCid, pgPool, 10000, now)
    assert.strictEqual(payloadsCalled, 1)
    deal.last_payload_retrieval_attempt = new Date(now)
    deal.payload_cid = payloadCid
    deal.payload_retrievability_state = PayloadRetrievabilityState.Resolved
    // The second attempt at retrieving the payload cid was successful and this should be reflected in the database entry
    assert.deepStrictEqual((await loadDeals(pgPool, 'SELECT * FROM active_deals'))[0], deal)

    // Now the piece indexer should no longer call the payload request for this deal
    await resolvePayloadCids(fetchMinerId, resolvePayloadCid, pgPool, 10000, now)
    assert.strictEqual(payloadsCalled, 1)
  })
})
