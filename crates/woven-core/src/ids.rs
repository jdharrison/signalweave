use std::fmt;

macro_rules! define_id {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            Eq,
            Hash,
            Ord,
            PartialEq,
            PartialOrd,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct $name(u64);

        impl $name {
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

define_id!(NamespaceId);
define_id!(SessionId);
define_id!(SpaceId);
define_id!(EntityId);
define_id!(ConnectionId);
define_id!(PrincipalId);
define_id!(ChannelId);
define_id!(SpaceEpoch);
define_id!(NodeId);

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct SessionKey {
    pub namespace: NamespaceId,
    pub session: SessionId,
}

impl SessionKey {
    #[must_use]
    pub const fn new(namespace: NamespaceId, session: SessionId) -> Self {
        Self { namespace, session }
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct SpaceKey {
    pub session: SessionKey,
    pub space: SpaceId,
}

impl SpaceKey {
    #[must_use]
    pub const fn new(session: SessionKey, space: SpaceId) -> Self {
        Self { session, space }
    }
}
