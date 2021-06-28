from motorway.pipeline import Pipeline


class ControllerPipeline(Pipeline):
    def definition(self):
        pass


if __name__ == '__main__':
    ControllerPipeline(run_controller=True, run_webserver=False, run_connection_discovery=False, controller_bind_address="connections:7007").run()
