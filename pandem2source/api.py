import tornado.escape
import tornado.ioloop
import tornado.web
from . import worker
from . import util
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
        self._variables_proxy = self._orchestrator_proxy.get_actor('variables').get().proxy()
        try:
            logger.debug("Starting HTTP server")
            sockets = tornado.netutil.bind_sockets(self._api_port) 
            self.server = HttpServer(
                storage_proxy=self._storage_proxy,
                variables_proxy = self._variables_proxy,
                sockets=sockets
            )
            self.server.start()
        except OSError as err:
            logger.debug(f"HTTP server startup failed: {err}")

    def on_stop(self):
        self.server.stop()


class HttpServer(threading.Thread):

    def __init__(self, storage_proxy, variables_proxy, sockets): 
        super().__init__()
        self.storage_proxy = storage_proxy
        self.variables_proxy = variables_proxy
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
        self.app = Application(self.storage_proxy, self.variables_proxy)
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
        tags = {}
        sources = {s['name']:s for s in (df_sources.to_dict('records') if df_sources is not None else [])} 
        jobs = df_jobs.to_dict('records') if df_jobs is not None else []
        
        for j in jobs:
          source = sources[j['source']]
          is_last_job = len([jj for jj in jobs if j["source"] == jj["source"] and jj["start_on"] > j["start_on"]]) == 0
          dls = j['dls_json']
          tag_name = dls["scope"]["tags"][0] if "tags" in dls["scope"] and len(dls["scope"]["tags"]) > 0 else source["name"]
          if tag_name in tags:
            tag = tags[tag_name]
          else:
            tag = {
              "name":tag_name,
              "files":0,
              "size":0,
              "progress":0.0,
              "issues":0,
              "last_import_start":None,
              "last_import_end":None,
              "next_check":None
            }
            tags[tag_name] = tag
            if source["next_exec"] is not None and (tag["next_check"] is None or tag["next_check"] > str(source["next_exec"])):
              tag["next_check"] = str(source["next_exec"])
            
            if is_last_job:
              tag["progress"] = (tag["progress"]*tag["files"] + j['progress']*len(j["source_files"]))/(tag["files"] + len(j["source_files"]))
              tag["files"] = tag["files"] + len(j["source_files"])
              tag["size"] =  tag["size"] + sum(j["file_sizes"])
              tag["issues"] =  tag["issues"] + len(df_issues[df_issues['job_id']==j["id"]] if df_issues is not None else [])
              
              if tag["last_import_start"] is None or tag['last_import_start'] < str(j['start_on']):
                  tag["last_import_start"] = str(j["start_on"])
              if "step" not in tag or tag["progress"] < j["progress"]:
                  tag['status'] = j['status']
                  tag['steps'] = j["step"]
            else:
              if tag["last_import_end"] is None or (j["end_on"] is not None and str(j["end_on"]) > tag["last_import_end"]):
                source['last_import_end'] = str(j['end_on'])
              
        res = list(tags.values())
        res.sort(key = lambda s:s["name"]) 
        response = {'sources' : res}
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
            required: false
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
        source = self.get_argument('source', default = None)
        sources = list(self.storage_proxy.read_db('source').get()['name'])
        if source is not None and source not in sources:
            self.set_status(400)
            return self.write("Invalid or not found source")
        else:
            df_issues = self.storage_proxy.read_db('issue').get()
            df_jobs = self.storage_proxy.read_db('job').get()
            df_jobs_source = df_jobs
            if source is not None:
              df_jobs_source = df_jobs[df_jobs['source']==source]
            jobs_list = [
              {'id': df_jobs_source['id'].iloc[i],
                  'source': str(df_jobs_source['source'].iloc[i]),
                  'start': str(df_jobs_source['start_on'].iloc[i]),
                  'end': str(df_jobs_source['end_on'].iloc[i]),
                  'status': df_jobs_source['status'].iloc[i],
                  'step': df_jobs_source['step'].iloc[i],
                  'dls': df_jobs_source['dls_json'].iloc[i],
                  'issues number': len(df_issues[(df_issues['source']==df_jobs_source['source'].iloc[i]) & (df_issues['job_id']==df_jobs_source['id'].iloc[i])]) if df_issues is not None else 0
              } 
              for i in range(len(df_jobs_source))
            ]
        response = {'Jobs' : jobs_list}
        self.write(response)

class IssuesHandler(tornado.web.RequestHandler):
    def initialize(self, storage_proxy):
        self.storage_proxy = storage_proxy
    def get(self):
        """
        ---
        tags:
          - Issues
        summary: List of issues realted to a Job of a source
        description: List of issues produced  for a particular job in a source.
        operationId: getIssues
        parameters:
          - name: source
            in: query
            description: source name
            required: false
            schema:
              type: string
          - name: job
            in: query
            description: job id
            required: false
            schema:
              type: int
        responses:
            '200':
              description: List of issues found during job of sources
              content:
                application/json:
                  schema:
                    $ref: '#/components/schemas/IssueModel'
                application/xml:
                  schema:
                    $ref: '#/components/schemas/IssueModel'
                text/plain:
                  schema:
                    type: string
        """
        source = self.get_argument('source', default = None)
        job = self.get_argument('job', default = None)

        sources = list(self.storage_proxy.read_db('source').get()['name'])
        if source is not None and source not in sources:
            self.set_status(400)
            return self.write("Invalid or not found source")
        else:
            df_jobs = self.storage_proxy.read_db('job').get()
            if source is not None:
              df_jobs_source = df_jobs[df_jobs['source']==source]
            if job is not None:
              df_jobs_source = df_jobs[df_jobs['id']==job]

            df_issues = self.storage_proxy.read_db('issue').get()
            if df_issues is not None:
              df_issues = df_issues[df_issues['job_id'].isin(df_jobs['id'])]
              df_issues = df_issues.copy()
              df_issues["raised_on"] = df_issues["raised_on"].astype(str)
              issue_list = df_issues.to_dict('records') 
            else:
              issue_list = []
        response = {'Issues' : issue_list}
        self.write(response)


class SourceDetailsHandler(tornado.web.RequestHandler):
    def initialize(self, storage_proxy):
        self.storage_proxy = storage_proxy
    def get(self):
        """
        ---
        tags:
          - Source Details
        summary: List of source details as per data labelling schema
        description: List of sources detauks as oer data labelling schema
        operationId: getSourceDetails
        parameters:
          - name: source
            in: query
            description: source name
            required: false
            schema:
              type: string
        responses:
            '200':
              description: List of data labelling schema for the sources
              content:
                application/json:
                  schema:
                    $ref: '#/components/schemas/SourceDetailModel'
                application/xml:
                  schema:
                    $ref: '#/components/schemas/SourceDetailModel'
                text/plain:
                  schema:
                    type: string
        """
        source = self.get_argument('source', default = None)
        sources_path = util.pandem_path("files", "source-definitions")
        dls_paths = self.storage_proxy.list_files(sources_path).get()
        dlss = [self.storage_proxy.read_file(f['path']).get() for f in dls_paths]
        defs = {dls["scope"]["tags"][0]+ " - "+ dls["scope"]["source"] if "tags" in dls["scope"] and len(dls["scope"]["tags"]) > 0 else dls["scope"]["source"]:dls for dls in dlss}
        if source is not None:
          defs = {k:v for k, v in defs.items() if k == source}
        response = {'sources':list(defs.keys()), 'definitions':defs }
        self.write(response)

class VariableListHandler(tornado.web.RequestHandler):
    def initialize(self, storage_proxy, variables_proxy):
        self.storage_proxy = storage_proxy
        self.variables_proxy = variables_proxy
    def get(self):
        """
        ---
        tags:
          - Variable List
        summary: List of used variables on this PANDEM-2 instance
        description: List variables published on this PANDEM-2 instace
        operationId: getVariableList
        parameters:
        responses:
            '200':
              description: List of variables and mofifiers
              content:
                application/json:
                  schema:
                    $ref: '#/components/schemas/VariableListModel'
                application/xml:
                  schema:
                    $ref: '#/components/schemas/VariableListModel'
                text/plain:
                  schema:
                    type: string
        """


        var_path = util.pandem_path("files", "variables")
        var_paths = self.storage_proxy.list_files(var_path, include_files = False, include_dirs = True, recursive = False).get()
        var_dic = self.variables_proxy.get_variables().get()
        
        pub_vars = {v['path'] for v in var_paths }

        sources_path = util.pandem_path("files", "source-definitions")
        dls_paths = self.storage_proxy.list_files(sources_path).get()
        dlss = [self.storage_proxy.read_file(f['path']).get() for f in dls_paths]
        used_vars = {col["variable"] for dls in dlss for col in dls['columns'] if "variable" in col}
        found_vars = {}
        for v in var_dic:
          if v in used_vars or v in pub_vars: 
             found_vars[v] = var_dic[v].copy()
             found_vars[v].pop("aliases")
             found_vars[v]["base_variable"] = found_vars[v]["variable"] if  found_vars[v]["variable"] != v else None
             found_vars[v]["variable"] = v
        ret = list(found_vars.values())
        ret.sort(key = lambda v: v["data_family"]+"_"+v["variable"])
        response = {'variables':ret}
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

@components.schemas.register
class IssueModel(object):
    """
    ---
    type: object
    description: Issue model representation
    properties:
        Issues:
            type: array
    """

@components.schemas.register
class SourceDetailModel(object):
    """
    ---
    type: object
    description: Source detail model representation
    properties:
        sources:
            type: array
        definitions:
            type: dict
    """

@components.schemas.register
class VariableListModel(object):
    """
    ---
    type: object
    description: Variable List model representation
    properties:
        variables:
            type: array
    """

class Application(tornado.web.Application):

    def __init__(self, storage_proxy, variables_proxy):
        settings = {"debug": True}
        self._routes = [
          tornado.web.url(r"/jobs", JobsBySourceHandler, {'storage_proxy': storage_proxy}),
          tornado.web.url(r"/sources", SourcesHandler, {'storage_proxy': storage_proxy}),
          tornado.web.url(r"/issues", IssuesHandler, {'storage_proxy': storage_proxy}),
          tornado.web.url(r"/source_details", SourceDetailsHandler, {'storage_proxy': storage_proxy}),
          tornado.web.url(r"/variable_list", VariableListHandler, {'storage_proxy': storage_proxy, 'variables_proxy':variables_proxy})
        ]
        setup_swagger(
            self._routes,
            swagger_url="/",
            description="",
            api_version="1.0.0",
            title="Pandem source API"
        )
        super(Application, self).__init__(self._routes, **settings)
