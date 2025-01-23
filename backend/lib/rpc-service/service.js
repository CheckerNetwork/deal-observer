import { RPC_URL } from '../config.js'
import { base64pad } from 'multiformats/bases/base64'
import { encode as cborEncode } from '@ipld/dag-cbor'
import { decode as jsonDecode } from '@ipld/dag-json'
import { request } from 'undici'
import { rawEventEntriesToEvent } from './utils.js'
import { Value } from '@sinclair/typebox/value'
import { ClaimEvent, RawActorEvent, BlockEvent } from './data-types.js'

/** @import {CID} from 'multiformats' */

/**
 * @param {string} method
 * @param {Object} params
  * @returns {Promise<object>}
  */
export const rpcRequest = async (method, params) => {
  const reqBody = JSON.stringify({ method, params, id: 1, jsonrpc: '2.0' })
  const response = await request(RPC_URL, {
    bodyTimeout: 1000 * 60,
    headersTimeout: 1000 * 60,
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: reqBody
  })
  return jsonDecode(new Uint8Array(await response.body.arrayBuffer())).result
}
/**
     * @param {object} actorEventFilter
     * Returns actor events filtered by the given actorEventFilter
     * @returns {Promise<Array<BlockEvent>>}
     */
export async function getActorEvents (actorEventFilter, makeRpcRequest) {
  const rawEvents = await makeRpcRequest('Filecoin.GetActorEventsRaw', [actorEventFilter])
  if (!rawEvents || rawEvents.length === 0) {
    console.log(`No actor events found in the height range ${actorEventFilter.fromHeight} - ${actorEventFilter.toHeight}.`)
    return []
  }
  // TODO: handle reverted events
  const typedRawEventEntries = rawEvents.map((rawEvent) => Value.Parse(RawActorEvent, rawEvent))
  // An emitted event contains the height at which it was emitted, the emitter and the event itself
  const emittedEvents = []
  for (const typedEventEntries of typedRawEventEntries) {
    const { event, eventType } = rawEventEntriesToEvent(typedEventEntries.entries)
    // Verify the returned event matches the expected event schema
    let typedEvent
    switch (eventType) {
      case 'claim': {
        typedEvent = Value.Parse(ClaimEvent, event)
        emittedEvents.push(
          Value.Parse(BlockEvent,
            {
              height: typedEventEntries.height,
              emitter: typedEventEntries.emitter,
              event: typedEvent
            }))
        continue
      }
      default: {
        console.error(`Unknown event type: ${eventType}`)
        break
      }
    }
  }
  return emittedEvents
}

/**
 * @param {function} makeRpcRequest
 * @returns {Promise<object>}
 */
export async function getChainHead (makeRpcRequest) {
  return await makeRpcRequest('Filecoin.ChainHead', [])
}

/**
 * @param {number} minerId
 * @param {function} rpcRequestFn
 * @returns {Promise<string>} 
 */
export async function getMinerPeerId(minerId, rpcRequestFn) {
  try {
    const params = getMinderInfoParameters(minerId)
    const res = await rpcRequestFn('Filecoin.StateMinerInfo', params)
    return res.PeerId
  } catch (err) {
    console.error(`Failed to get peer ID for miner ${minerId}:`, err)
  }
}

/**
   * @param {number} blockHeight
   * @param {string} eventTypeString
   */
export function getActorEventsFilter (blockHeight, eventTypeString) {
  // We only search for events in a single block
  return {
    fromHeight: blockHeight,
    toHeight: blockHeight,
    fields: {
      $type: // string must be encoded as CBOR and then presented as a base64 encoded string
        // Codec 81 is CBOR and will only give us builtin-actor events, FEVM events are all RAW
        [{ Codec: 81, Value: base64pad.baseEncode(cborEncode(eventTypeString)) }]
    }
  }
}

/**
 * 
 * @param {number} minerId 
 */
export function getMinderInfoParameters(minerId){
  return [
    'f0'+minerId.toString(),
    null
  ]
} 
