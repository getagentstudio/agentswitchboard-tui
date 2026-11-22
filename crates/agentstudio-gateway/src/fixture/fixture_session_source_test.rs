#![allow(clippy::unwrap_used)]

use futures_util::StreamExt;

use super::*;

#[tokio::test]
async fn one_cut_buffers_live_independently_for_two_subscriptions() {
    let source = Arc::new(FixtureSessionSource::default());
    let cancellation = CancellationToken::new();
    let request = ObserveArguments {
        target: fixture_target(),
        timeout_seconds: 5,
        max_events: 20,
        max_bytes: 2048,
        after_sequence: None,
        epoch: None,
    };
    let mut first = source
        .observe(request.clone(), cancellation.clone())
        .await
        .unwrap();
    let mut second = source
        .observe(request, CancellationToken::new())
        .await
        .unwrap();

    source
        .append_notice("Arrived after subscription, before history was read".into())
        .unwrap();

    for subscription in [&mut first, &mut second] {
        assert!(matches!(
            subscription.next().await,
            Some(SessionObservation::History {
                cursor: Cursor {
                    sequence: 9_007_199_254_740_991,
                    ..
                },
                ..
            })
        ));
        assert!(matches!(
            subscription.next().await,
            Some(SessionObservation::History {
                cursor: Cursor {
                    sequence: 9_007_199_254_740_992,
                    ..
                },
                ..
            })
        ));
        assert!(matches!(
            subscription.next().await,
            Some(SessionObservation::SnapshotEnd(Cursor {
                sequence: 9_007_199_254_740_992,
                ..
            }))
        ));
        assert!(matches!(
            subscription.next().await,
            Some(SessionObservation::Live {
                cursor: Cursor {
                    sequence: 9_007_199_254_740_993,
                    ..
                },
                ..
            })
        ));
    }

    cancellation.cancel();
    assert!(matches!(
        first.next().await,
        Some(SessionObservation::Ended(EndReason::Cancelled))
    ));
    assert!(first.next().await.is_none());
    source
        .append_notice("Visible only to the remaining subscription".into())
        .unwrap();
    assert!(matches!(
        second.next().await,
        Some(SessionObservation::Live {
            cursor: Cursor {
                sequence: 9_007_199_254_740_994,
                ..
            },
            ..
        })
    ));
}
