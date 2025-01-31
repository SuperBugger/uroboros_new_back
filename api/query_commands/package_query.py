from ..query_commands.base_query import BaseApi, time_decorator


# pkg_vrs_id and date
class PackageApi(BaseApi):
    def __init__(self, db_helper):
        super().__init__()
        self._db_helper = db_helper
        self.table_name = "repositories.pkg_version"
        self.name_col = ["pkg_vrs_id", "author_name", "assm_id", 'assm_date_created', "prj_name", "prj_id", "pkg_name",
                         "version", "date_created"]
        self.fields = ("rpv.pkg_vrs_id, rpv.author_name, ra.assm_id, ra.assm_date_created, "
                       "rpp.prj_name, ra.prj_id, rp.pkg_name, rpv.version, rpv.pkg_date_created")
        self.join += (" rpv join repositories.assm_pkg_vrs rapv on rapv.pkg_vrs_id = rpv.pkg_vrs_id"
                      " join repositories.package rp on rpv.pkg_id = rp.pkg_id"
                      " join repositories.assembly ra on ra.assm_id=rapv.assm_id "
                      " join repositories.project rpp on rpp.prj_id=ra.prj_id")

    @time_decorator
    def delete_table(self, name):
        try:
            sql = f"SELECT EXISTS(SELECT * FROM pg_tables WHERE tablename = '{name}'); "
            if self._db_helper.query(sql)[0][0]:
                sql = f"DELETE FROM maintenance.{name}"
                self._db_helper.query(sql)
                self._db_helper.commit_conn()
            else:
                print("Table doesn't exist")
        except Exception as e:
            self._error(f" while drop table {e}")

    def create_query(self, args):
        self.where += f"rapv.assm_id = {args.assm_id}"

    @time_decorator
    def joint_assm(self, args):
        try:
            sql = "SELECT EXISTS(SELECT * FROM pg_tables WHERE tablename = 'assm_vul'); "
            if not self._db_helper.query(sql)[0][0]:
                sql = (" create table maintenance.assm_vul("
                       " pkg_vul_id integer, prj_name text, pkg_name text, author_name text, "
                       " joint_vers text, "
                       "assm_date timestamptz, "
                       "cve_vers text, "
                       "cve_name text, cve_desc text, st_name text, "
                       "urg_name text, rep_name text, link text, "
                       "vul_ident text, vul_name text, vul_desc text, "
                       "date_discovered timestamptz, cvss2_vector text, cvss2_score text, "
                       "cvss3_vector text, cvss3_score text, "
                       "severity text, cve_name text, url text);")
                self._db_helper.query(sql)
                self._db_helper.commit_conn()
            else:
                sql = "select assm_id from maintenance.assm_vul where "
            sql = f"call maintenance.get_joint_assm({args.assm_id});"
            self._db_helper.query(sql)
            self._db_helper.commit_conn()
            sql = f"select pkg_vul_id, prj_name, pkg_name, author_name, joint_vers, assm_date from maintenance.assm_vul"
            self.name_col = ["pkg_vrs_id", "prj_name", "pkg_name", "author_name", "joint_vers", "assm_date"]
            self.query = sql
        except Exception as e:
            self._error(f" while join assm {e}")

    @time_decorator
    def get_difference(self, args):
        try:
            sql = "SELECT EXISTS(SELECT * FROM pg_tables WHERE tablename = 'compare_assm'); "
            if not self._db_helper.query(sql)[0][0]:
                sql = ("create table maintenance.compare_assm( pkg_name text, curr_vers text, "
                       "curr_date timestamp with time zone,"
                       "vers_status text,"
                       "assm_date timestamp with time zone,"
                       "pkg_vers text,"
                       "prev boolean,"
                       "current_assm boolean);")
                self._db_helper.query(sql)
                self._db_helper.commit_conn()
                print(args.assm_id)
            sql = f"call maintenance.compare_assm({args.assm_id})"
            self._db_helper.query(sql)
            self._db_helper.commit_conn()
            sql = f"select * from maintenance.compare_assm where prev={args.prev} and current_assm = {args.current} "
            if args.dif_filter is not None:
                if len(args.dif_filter) != 0:
                    str_filter = ""
                    for x in range(0, len(args.dif_filter) - 1):
                        str_filter += "'" + args.dif_filter[x] + "'" + ","
                    str_filter += "'" + args.dif_filter[-1] + "'"
                    sql += f" and vers_status in ({str_filter})"
            self.name_col = ["pkg_name", "curr_vers", "curr_date", "vers_status", "assm_date", "pkg_ver"]
            self.query = sql
        except Exception as e:
            self._error(f" while get difference {e}")

    def run(self, assm):
        try:
            print('sssssssssssss')
            if assm.assm_id is not None:
                print('sssssssssssss')
                if assm.difference:
                    if assm.delete:
                        self.delete_table("compare_assm")
                    else:
                        self.get_difference(assm)
                        self.run_query(build=False, t_id=False)
                if assm.joint:
                    if assm.delete:
                        self.delete_table("assm_vul")
                    else:
                        self.joint_assm(assm)
                        self.run_query(build=False)
                if not assm.joint and not assm.difference:
                    self.create_query(assm)
                    self.run_query()
                print(self.tbl_dict)
                return self.tbl_dict
            else:
                self._error("assm_id don't null")
        except Exception as e:
            self._error(e)
