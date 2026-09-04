//! Canonical cross-language transform payload encoding.

/// A spatial transform encoded as ten little-endian IEEE-754 `f32` values.
///
/// The wire order is translation `x,y,z`, quaternion rotation `x,y,z,w`, then scale `x,y,z`.
/// Applications configure the session's transform route; this type only defines the opaque
/// payload carried by that route.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Transform {
    pub translation: [f32; 3],
    pub rotation: [f32; 4],
    pub scale: [f32; 3],
}

impl Transform {
    /// Fixed encoded payload length: ten `f32` values.
    pub const ENCODED_LEN: usize = 40;

    /// Encodes this transform in the canonical little-endian representation.
    #[must_use]
    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut output = [0; Self::ENCODED_LEN];
        for (index, value) in self.values().into_iter().enumerate() {
            let start = index * 4;
            output[start..start + 4].copy_from_slice(&value.to_le_bytes());
        }
        output
    }

    /// Decodes a canonical transform payload.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        let mut values = [0.0; 10];
        for (index, value) in values.iter_mut().enumerate() {
            let start = index * 4;
            *value = f32::from_le_bytes(bytes[start..start + 4].try_into().ok()?);
        }
        Some(Self {
            translation: [values[0], values[1], values[2]],
            rotation: [values[3], values[4], values[5], values[6]],
            scale: [values[7], values[8], values[9]],
        })
    }

    fn values(self) -> [f32; 10] {
        [
            self.translation[0],
            self.translation[1],
            self.translation[2],
            self.rotation[0],
            self.rotation[1],
            self.rotation[2],
            self.rotation[3],
            self.scale[0],
            self.scale[1],
            self.scale[2],
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::Transform;

    #[test]
    fn round_trips_canonical_payload() {
        let transform = Transform {
            translation: [1.0, -2.0, 3.5],
            rotation: [0.0, 0.0, 0.5, 0.5],
            scale: [1.0, 2.0, 1.0],
        };
        let encoded = transform.encode();
        assert_eq!(encoded.len(), Transform::ENCODED_LEN);
        assert_eq!(Transform::decode(&encoded), Some(transform));
        assert_eq!(&encoded[..4], &1.0_f32.to_le_bytes());
    }
}
