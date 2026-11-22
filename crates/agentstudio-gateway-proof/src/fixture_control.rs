use std::{
    io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    time,
};
use tokio_util::sync::CancellationToken;

use crate::{peer_transport::PeerControls, witness_fixture::WitnessFixture};

const CONTROL_BYTES: u64 = 16 * 1024;
const CONTROL_DEADLINE: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum FixtureCommand {
    ReleaseFirst,
    ReleaseCut,
    ReleaseLive,
    Publish { text: String },
    Abort { stream: u64 },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum FixtureReply {
    Applied,
    Published { sequence: u64 },
    Refused,
}

pub struct FixtureControl {
    listener: UnixListener,
    socket: PathBuf,
    fixture: WitnessFixture,
    peer: PeerControls,
}

impl FixtureControl {
    pub async fn bind(
        socket: &Path,
        fixture: WitnessFixture,
        peer: PeerControls,
    ) -> Result<Self, io::Error> {
        let listener = UnixListener::bind(socket)?;
        let control = Self {
            listener,
            socket: socket.into(),
            fixture,
            peer,
        };
        tokio::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600)).await?;
        Ok(control)
    }

    pub async fn serve(self, stop: CancellationToken) -> Result<(), io::Error> {
        loop {
            let (mut client, _) = tokio::select! {
                () = stop.cancelled() => return Ok(()),
                accepted = self.listener.accept() => accepted?,
            };
            let result = time::timeout(CONTROL_DEADLINE, self.handle(&mut client));
            tokio::select! {
                () = stop.cancelled() => return Ok(()),
                _ = result => {}
            }
        }
    }

    async fn handle(&self, client: &mut UnixStream) -> Result<(), io::Error> {
        let mut command = Vec::new();
        (&mut *client)
            .take(CONTROL_BYTES + 1)
            .read_to_end(&mut command)
            .await?;
        let reply = if command.len() as u64 <= CONTROL_BYTES {
            serde_json::from_slice(&command)
                .map_or(FixtureReply::Refused, |command| self.apply(command))
        } else {
            FixtureReply::Refused
        };
        client.write_all(&serde_json::to_vec(&reply)?).await?;
        client.shutdown().await
    }

    fn apply(&self, command: FixtureCommand) -> FixtureReply {
        let changed = match command {
            FixtureCommand::ReleaseFirst => self.fixture.release_first(),
            FixtureCommand::ReleaseCut => self.fixture.release_cut(),
            FixtureCommand::ReleaseLive => self.fixture.release_live(),
            FixtureCommand::Abort { stream } => self.peer.abort_stream(stream),
            FixtureCommand::Publish { text } => {
                return self
                    .fixture
                    .publish(text)
                    .map_or(FixtureReply::Refused, |sequence| FixtureReply::Published {
                        sequence,
                    });
            }
        };
        if changed {
            FixtureReply::Applied
        } else {
            FixtureReply::Refused
        }
    }
}

impl Drop for FixtureControl {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket);
    }
}

pub async fn control_call(socket: &Path, command: &[u8]) -> Result<String, io::Error> {
    time::timeout(CONTROL_DEADLINE, async {
        let mut connection = UnixStream::connect(socket).await?;
        connection.write_all(command).await?;
        connection.shutdown().await?;
        let mut response = String::new();
        connection
            .take(CONTROL_BYTES)
            .read_to_string(&mut response)
            .await?;
        Ok(response)
    })
    .await?
}
