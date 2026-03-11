from .session import TrainingSession


def run():
    print("Opening Trainer v1")

    session = TrainingSession()

    while True:
        session.start_new_game()
        session.run_session()
