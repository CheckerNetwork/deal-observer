import { Type } from '@sinclair/typebox'
import { Value } from '@sinclair/typebox/value'
import { PIECE_INDEXER_URL } from './config.js'
import pRetry from 'p-retry'

const PieceIndexerResponse = Type.Object({
  samples: Type.Array(Type.String())
})

const PieceIndexerErrorResponse = Type.Object({
  error: Type.String()
})

/**
 * @param {string} providerId
 * @param {string} pieceCid
 * @returns {Promise<string|null>}
 */
export const resolvePayloadCid = async (providerId, pieceCid) => {
  const url = PIECE_INDEXER_URL + '/sample/' + providerId + '/' + pieceCid
  try {
    const response = await pRetry(async () => await fetch(url, {
      method: 'GET',
      headers: { 'content-type': 'application/json' }
    }), { retries: 5 })
    const json = await response.json()

    try {
      const parsedPixResponse = Value.Parse(PieceIndexerErrorResponse, json)
      if (parsedPixResponse.error === 'PROVIDER_OR_PIECE_NOT_FOUND') {
        return null
      }
    } catch { }

    try {
      const parsedPixResponse = Value.Parse(PieceIndexerResponse, json)
      return parsedPixResponse.samples.length === 0
        ? null
        : parsedPixResponse.samples[0]
    } catch (e) {
      throw new Error(`Failed to parse response from piece indexer. The response was : ${JSON.stringify(json)}`, { cause: e })
    }
  } catch (e) {
    throw new Error('Failed to make RPC request.', { cause: e })
  }
}
