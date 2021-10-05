use uuid::Uuid;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Peer {
    uuid: Uuid,
    name: String,
}

impl Peer {
    pub fn new(name: String) -> Self {
        let uuid = Uuid::new_v4();
        Self {
            uuid,
            name,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> Uuid {
        self.uuid
    }
}

