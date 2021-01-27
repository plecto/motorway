from contextlib import contextmanager
import os
NEW_RELIC = False
try:
    import newrelic.agent
    if os.environ.get("NEW_RELIC_CONFIG_FILE"):
        NEW_RELIC = True
except ImportError:
    pass


@contextmanager
def instrumentation_manager(task_name):
    if NEW_RELIC:
        with newrelic.agent.BackgroundTask(newrelic.agent.application(), task_name):
            yield
    else:
        yield
