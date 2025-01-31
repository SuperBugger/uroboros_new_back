from ..query_commands.base_query import BaseApi, time_decorator


class ProjectApi(BaseApi):
    def __init__(self, db_helper):
        super().__init__()
        self._db_helper = db_helper
        self.table_name = "repositories.project"
        self.name_col = ["prj_id", "prj_name", "rel_name", "prj_desc", "vendor", "arch_name"]
        self.fields = "prj_id, prj_name, rel_name, prj_desc, vendor, arch_name"
        self.prj_dict = {"prj_id": None, "prj_name": "", "rel_name": None,
                         "prj_desc": "", "vendor": "", "arch_name": None}
        self.join += (" rp left join repositories.release rr on rp.rel_id = rr.rel_id"
                      " left join repositories.architecture ra on ra.arch_id = rp.arch_id")

    @time_decorator
    def create_query(self, tbl_id):
        if tbl_id is not None:
            self.where += f"prj_id = {tbl_id}"
        else:
            self.where = ""

    @time_decorator
    def get_projects(self):
        try:
            sql = """
                    SELECT
                        p.prj_name,
                        a.arch_name,
                        r.rel_name,
                        p.vendor,
                        p.prj_desc
                    FROM
                        repositories.project p
                    LEFT JOIN
                        repositories.architecture a ON p.arch_id = a.arch_id
                    LEFT JOIN
                        repositories.release r ON p.rel_id = r.rel_id
                """
            result = self._db_helper.query(sql)
            projects = [
                {
                    'prj_name': row[0],
                    'arch_name': row[1],
                    'rel_name': row[2],
                    'vendor': row[3],
                    'prj_desc': row[4]
                }
                for row in result
            ]
            print("Projects from get_projects", projects)
            return projects
        except Exception as e:
            self._error(f" while fetching projects {e}")
            return []

    def run(self, prj_id):
        try:
            self.create_query(prj_id)
            self.run_query()
            return self.tbl_dict
        except Exception as e:
            self._error(e)
