from contextlib import contextmanager

NEW_RELIC = False
try:
    import newrelic.agent
    if newrelic.agent.application().active:
        NEW_RELIC = True
except ImportError:
    pass


@contextmanager
def instrumentation_manager(task_name):
    if NEW_RELIC:
        yield newrelic.agent.BackgroundTask(newrelic.agent.application(), task_name)
    else:
        yield EmptyInstrumentationManager()


class EmptyInstrumentationManager(object):
    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, type, val, traceback):
        pass
