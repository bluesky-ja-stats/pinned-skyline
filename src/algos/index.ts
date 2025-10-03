import {
  type QueryParams,
  type OutputSchema as AlgoOutput,
} from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { type AppContext } from '../util/config'
import * as pinnedSkylineJa from './pinned-skyline-ja'

type AlgoHandler = (ctx: AppContext, params: QueryParams) => Promise<AlgoOutput>

const algos: Record<string, AlgoHandler> = {
  [pinnedSkylineJa.shortname]: pinnedSkylineJa.handler,
}

export default algos
