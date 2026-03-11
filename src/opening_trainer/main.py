from .session import TrainingSession


def run() -> None:
    print("Opening Trainer v1", flush=True)

    session = TrainingSession()

    while True:
        session.start_new_game()
        session.run_session()
