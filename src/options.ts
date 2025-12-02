export type PrepareCacheOption = boolean | number | { size?: number };

export interface PreparedStatementCacheConfig {
  size: number;
}

const DEFAULT_PREPARED_CACHE_SIZE = 32;

export function resolvePrepareCacheOption(
  option?: PrepareCacheOption
): PreparedStatementCacheConfig | undefined {
  if (!option) return undefined;

  if (option === true) {
    return { size: DEFAULT_PREPARED_CACHE_SIZE };
  }

  if (typeof option === 'number') {
    const size = Math.max(1, Math.floor(option));
    return { size };
  }

  const size = option.size ?? DEFAULT_PREPARED_CACHE_SIZE;
  return { size: Math.max(1, Math.floor(size)) };
}
