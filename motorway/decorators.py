import logging
import traceback

logger = logging.getLogger(__name__)

def batch_process(wait=5, limit=100):
    def inner(method):
        method.batch_process = True
        method.wait = wait
        method.limit = limit
        return method
    return inner


def post_error_to_sentry(client):
    def inner(method):
        method.post_error_to_sentry = True
        return method
    return inner
    # def inner(method):
    #     def dec(*args, **kwargs):
    #         try:
    #             logger.info("Running method()")
    #             return method(*args, **kwargs)
    #         except:
    #             logger.error("Caught exception", exc_info=True)
    #             print "this happened"
    #             identifier = client.get_ident(client.captureException())
    #             # print traceback.format_exc()
    #             # print "Exception caught; reference is %s" % identifier
    #             # raise
    #     # print method, dec
    #     return dec
    # return inner