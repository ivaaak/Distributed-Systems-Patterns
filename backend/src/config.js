export const PORT = Number(process.env.PORT || 4001);
export const INSTANCE_ID = process.env.INSTANCE_ID || `api-${PORT}`;
// The URL a *browser* uses to reach this instance (differs from the in-cluster address under Docker).
export const PUBLIC_URL = process.env.PUBLIC_URL || `http://localhost:${PORT}`;
// The URL *other instances* use to reach this one (e.g. http://api-2:4002 inside Docker).
export const INTERNAL_URL = process.env.INTERNAL_URL || PUBLIC_URL;
export const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:16379';
