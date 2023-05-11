# used by process hub core to signal core exit
class SelfExitException(Exception):
    pass


class GenericExitOnSignal(Exception):
    # to be used whenever any other launched process/callback worker receives a signal to 'exit' - [INT, TERM, QUIT]
    pass
