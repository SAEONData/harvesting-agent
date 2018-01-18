#!/usr/bin/env python
import cherrypy
from agent.agent import Agent


class AgentAPI:

    index_page = """
    <!DOCTYPE html>
    <html>
    <head><title>Agent API</title></head>
    <body>
        Invoke a Harvester with the following input:
        <form method="post" action="invoke_harvester">
            <table>
            {input_fields}
            </table>
            <input type="submit" value="Submit"/>
        </form>
    </body>
    </html>
    """

    invoke_harvester_params = (
        'harvester_uid',
        'datasource_uid',
        'repository_uid',
        'repository_url',
        'username',
        'password',
        'institution',
    )

    # region Public API

    @cherrypy.expose
    def index(self):
        return self.index_page.format(input_fields=self._make_input_fields(self.invoke_harvester_params))

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def invoke_harvester(self, **kwargs):
        param_values = self._get_params(self.invoke_harvester_params, kwargs)
        success, message = Agent.invoke_harvester(*param_values)
        return {'success': success, 'message': message}

    # endregion

    @staticmethod
    def _get_params(param_names, input_args):
        param_values = []
        missing = []
        for param_name in param_names:
            if param_name in input_args:
                param_values += [input_args[param_name]]
            else:
                missing += [param_name]

        if missing:
            raise cherrypy.HTTPError(400, "Missing input arg(s): " + ", ".join(missing))

        return param_values

    @staticmethod
    def _make_input_fields(param_names):
        input_fields = ''
        for param_name in param_names:
            input_fields += '<tr><td>{0}:</td><td><input type="text" name="{0}"></td></tr>\n'.format(param_name)
        return input_fields


if __name__ == "__main__":
    cherrypy.config.update({'server.socket_port': 9090})
    cherrypy.quickstart(AgentAPI())
