export type RewriteArraysMode = 'auto' | 'always' | 'never';

export type RewriteArraysOption = boolean | RewriteArraysMode;

const DEFAULT_REWRITE_ARRAYS_MODE: RewriteArraysMode = 'auto';

export function resolveRewriteArraysOption(
  value?: RewriteArraysOption
): RewriteArraysMode {
  if (value === undefined) return DEFAULT_REWRITE_ARRAYS_MODE;
  if (value === true) return 'auto';
  if (value === false) return 'never';
  return value;
}

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
