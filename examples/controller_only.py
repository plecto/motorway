from motorway.pipeline import Pipeline


class ControllerPipeline(Pipeline):
    def definition(self):
        pass


ControllerPipeline(run_controller=True, run_webserver=False, run_connection_discovery=False, controller_bind_address="connections:7007").run()