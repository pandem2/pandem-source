import tornado.escape
import tornado.ioloop
import tornado.web
from . import worker
import threading
from abc import ABCMeta
import logging
import asyncio
import tornado.httpserver
import tornado.netutil
import tornado.websocket
from tornado_swagger.components import components
from tornado_swagger.setup import setup_swagger


logger = logging.getLogger(__name__)


class apiREST(worker.Worker):
    __metaclass__ = ABCMeta 

    def __init__ (self, name, orchestrator_ref, settings):
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
        self._api_port = settings["pandem"]["source"]["api"]["port"]
    
    def on_start(self):
        super().on_start()
        self._storage_proxy = self._orchestrator_proxy.get_actor('storage').get().proxy()
        try:
            logger.debug("Starting HTTP server")
            sockets = tornado.netutil.bind_sockets(self._api_port) 
            self.server = HttpServer(
                storage_proxy=self._storage_proxy,
                sockets=sockets
            )
        except OSError as err:
            logger.debug(f"HTTP server startup failed: {err}")
        self.server.start()

    def on_stop(self):
        self.server.stop()


class HttpServer(threading.Thread):

    def __init__(self, storage_proxy, sockets): 
        super().__init__()
        self.storage_proxy = storage_proxy
        self.sockets = sockets
        self.app = None
        self.server = None
        self.io_loop = None

    def run(self):
        if asyncio:
            # If asyncio is available, Tornado uses it as its IO loop. Since we
            # start Tornado in a another thread than the main thread, we must
            # explicitly create an asyncio loop for the current thread.
            asyncio.set_event_loop(asyncio.new_event_loop())
        self.app = Application(self.storage_proxy)
        self.server = tornado.httpserver.HTTPServer(self.app)
        self.server.add_sockets(self.sockets)
        self.io_loop = tornado.ioloop.IOLoop.current()
        self.io_loop.start()

    def stop(self):
        logger.debug("Stopping HTTP server")
        self.io_loop.add_callback(self.io_loop.stop)


class SourcesHandler(tornado.web.RequestHandler):
    def initialize(self, storage_proxy):
        self.storage_proxy = storage_proxy
    def get(self):
        """
        ---
        tags:
          - Sources
        summary: List sources
        description: List all sources in sources pickle file within the database.
        operationId: getSources
        responses:
            '200':
              description: List all sources including id, name, last and next execution, current status and step of last job and number of issues of last
              content:
                application/json:
                  schema:
                    $ref: '#/components/schemas/SourcesModel'
                application/xml:
                  schema:
                    $ref: '#/components/schemas/SourcesModel'
                text/plain:
                  schema:
                    type: string
        """
        df_sources = self.storage_proxy.read_db('source').get()
        df_jobs = self.storage_proxy.read_db('job').get()
        df_issues = self.storage_proxy.read_db('issue').get()
        if df_sources is None or df_jobs is None:
            self.set_status(400)
            return self.write("No files for jobs or sources")
        sources_list = []
        if df_sources is not None:
            sources_list = [{'id': df_sources.loc[i,'id'],
                            'name': df_sources.loc[i,'name'],
                            'last execution': str(df_sources.loc[i,'last_exec']),
                            'next execution': str(df_sources.loc[i,'next_exec'])
                            } 
                            for i in range(1, len(df_sources)+1)]
            if df_jobs is not None:
                for source in sources_list: 
                    if source['name'] in df_jobs['source'].values:
                        source['last job status'] = df_jobs[df_jobs['source']==source['name']]['status'].iloc[-1]
                        source['last job step'] = df_jobs[df_jobs['source']==source['name']]['step'].iloc[-1]

                    if df_issues is not None and source['name'] in df_issues['source']:
                        source['last job issues number'] = len(df_issues[df_issues['source']==source['name']])
                    else:
                        source['last job issues number'] = 0
        response = {'sources' : sources_list}
        self.write(response)

class JobsBySourceHandler(tornado.web.RequestHandler):
    def initialize(self, storage_proxy):
        self.storage_proxy = storage_proxy
    def get(self):
        """
        ---
        tags:
          - Jobs
        summary: List of jobs related to a source
        description: List of jobs for a particular source including job_id, start, end, status, step, dls file and number of issues.
        operationId: getJobs
        parameters:
          - name: source
            in: query
            description: source name
            required: true
            schema:
              type: string
        responses:
            '200':
              description: List of jobs for a particular source including job_id, start, end, status, step, dls, files, number of issues
              content:
                application/json:
                  schema:
                    $ref: '#/components/schemas/JobsModel'
                application/xml:
                  schema:
                    $ref: '#/components/schemas/JobsModel'
                text/plain:
                  schema:
                    type: string
        """
        source = self.get_argument('source')
        sources = list(self.storage_proxy.read_db('source').get()['name'])
        if source not in sources:
            self.set_status(400)
            return self.write("Invalid or not found source")
        else:
            df_issues = self.storage_proxy.read_db('issue').get()
            df_jobs = self.storage_proxy.read_db('job').get()
            df_jobs_source = df_jobs[df_jobs['source']==source]
            jobs_list = [{'id': df_jobs_source['id'].iloc[i],
                        'start': str(df_jobs_source['start_on'].iloc[i]),
                        'end': str(df_jobs_source['end_on'].iloc[i]),
                        'status': df_jobs_source['status'].iloc[i],
                        'step': df_jobs_source['step'].iloc[i],
                        'dls': df_jobs_source['dls_json'].iloc[i],
                        'issues number': len(df_issues[(df_issues['source']==source) & (df_issues['job_id']==df_jobs_source['id'].iloc[i])]) if df_issues else 0
                        } 
                        for i in range(len(df_jobs_source))]
        response = {'Jobs' : jobs_list}
        self.write(response)


@components.schemas.register
class SourcesModel(object):
    """
    ---
    type: object
    description: Sources model representation
    properties:
        sources:
            type: array
    """

@components.schemas.register
class JobsModel(object):
    """
    ---
    type: object
    description: Sources model representation
    properties:
        Jobs:
            type: array
    """


class Application(tornado.web.Application):

    def __init__(self, storage_proxy):
        settings = {"debug": True}
        self._routes = [tornado.web.url(r"/jobs", JobsBySourceHandler, {'storage_proxy': storage_proxy}),tornado.web.url(r"/sources", SourcesHandler, {'storage_proxy': storage_proxy})]
        setup_swagger(
            self._routes,
            swagger_url="/doc",
            description="",
            api_version="1.0.0",
            title="Journal API"
        )
        super(Application, self).__init__(self._routes, **settings)