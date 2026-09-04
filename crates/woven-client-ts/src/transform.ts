/**
 * Canonical Woven transform payload.
 *
 * The wire representation is ten little-endian IEEE-754 `f32` values in this order:
 * translation `x,y,z`, quaternion rotation `x,y,z,w`, then scale `x,y,z`.
 */
export interface Transform {
  translation: [number, number, number];
  rotation: [number, number, number, number];
  scale: [number, number, number];
}

/** Fixed encoded payload size: ten `f32` values. */
export const TRANSFORM_ENCODED_LENGTH = 40;

/** Encode a transform into Woven's canonical cross-language payload. */
export function encodeTransform(transform: Transform): Uint8Array {
  const output = new Uint8Array(TRANSFORM_ENCODED_LENGTH);
  const view = new DataView(output.buffer);
  const values = [...transform.translation, ...transform.rotation, ...transform.scale];
  values.forEach((value, index) => view.setFloat32(index * 4, value, true));
  return output;
}

/** Decode a canonical transform payload, returning `null` for an invalid length. */
export function decodeTransform(payload: Uint8Array): Transform | null {
  if (payload.byteLength !== TRANSFORM_ENCODED_LENGTH) return null;
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const values = Array.from({ length: 10 }, (_, index) => view.getFloat32(index * 4, true));
  return {
    translation: [values[0]!, values[1]!, values[2]!],
    rotation: [values[3]!, values[4]!, values[5]!, values[6]!],
    scale: [values[7]!, values[8]!, values[9]!],
  };
}
