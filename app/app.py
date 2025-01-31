import pathlib

import falcon
from falcon_cors import CORS

from resources import *
from resources import ChooseAddProjectResource, ProjectWrongPathResource, ProjectExisttResource, \
    ProjectAddPathResource, ProjectAddInputResource

cors = CORS(
    allow_origins_list=['http://0.0.0.0:8080'],
    allow_all_methods=True,
    allow_headers_list=['Content-Type']
)
app = falcon.App(middleware=[cors.middleware])


class IndexResource:
    def on_get(self, req, resp):
        resp.content_type = 'text/html'
        index_path = pathlib.Path(__file__).parent / 'static' / 'index.html'
        with open(index_path, 'r') as f:
            resp.text = f.read()


STATIC_PATH = pathlib.Path(__file__).parent / 'static'
print(STATIC_PATH)
app.add_static_route('/', str(STATIC_PATH))
app.add_route("/cve", CVEResource())
app.add_route('/bdu', BDURource())
app.add_route("/{resource}/{resource_id}/info", BreadcrumbResource())
app.add_route("/projects", ProjectResource())  #+
app.add_route("/projects/{prj_id}", ProjectResource()) #+
app.add_route("/projects/add", ChooseAddProjectResource())  #+
app.add_route('/projects/update', UpdateResource())
app.add_route("/projects/already_exist", ProjectExisttResource())  #+
app.add_route("/projects/wrong_path", ProjectWrongPathResource())  #+
app.add_route("/projects/add/path", ProjectAddPathResource())  #+
app.add_route("/projects/add/input", ProjectAddInputResource())  #+
app.add_route("/projects/{prj_id}/delete", ProjectDeleteResource())  #+
app.add_route('/projects/{prj_id}/assembly', AssemblyResource())  #+
app.add_route('/projects/{prj_id}/assembly/add', AddAssmResource())  #+
app.add_route('/projects/{prj_id}/assembly/{assm_id}/delete', DeleteAssmResource())  #+
app.add_route('/projects/{prj_id}/assembly/{assm_id}/vulnerability/{resolved}', AssemblyCveResource())  #+
app.add_route('/projects/{prj_id}/assembly/{assm_id}/joint', AssemblyJointResource())  #---
app.add_route('/projects/{prj_id}/assembly/{assm_id}/compare', AssemblyCompareResource())  #--
app.add_route('/projects/{prj_id}/assembly/{assm_id}/package', PackageResource())  #+
app.add_route('/projects/{prj_id}/assembly/{assm_id}/package/{pkg_name}/vulnerabilities', PackageCVEResource())
# app.add_route('/projects/{prj_id}/assembly/{assm_id}/package/{pkg_id}/vulnerabilities', PackageCVEResource())
# app.add_route('/projects/{prj_id}/assembly/{assm_id}/package/{pkg_id}/vulnerability',
#               PackageCveResource())  #+
app.add_route('/projects/{prj_id}/assembly/{assm_id}/joint/{pkg_vul_id}/vulnerability/{resolved}',
              JointCveResource())  #--
app.add_route('/projects/{prj_id}/assembly/{assm_id}/packages/{pkg_id}/changelog', ChangelogResource())  #
app.add_route('/', IndexResource())
# app.add_route('/{path}', IndexResource())
app.add_route('/api/stats', StatsResource())
app.add_route('/api/cve/{cve_name}/links', CVELinksResource())


