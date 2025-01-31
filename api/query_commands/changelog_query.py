from ..query_commands.base_query import BaseApi


class ChangelogApi(BaseApi):
    def __init__(self, db_helper):
        super().__init__()
        self._db_helper = db_helper
        self.table_name = "repositories.pkg_version rpv "
        self.name_col = ["id", "log_desc", "urg_name", "date_added",
                         "log_ident", "rep_name"]
        self.fields = "rc.id, rc.log_desc, ru.urg_name, rc.date_added, rc.log_ident, rc.rep_name"

    def create_query(self, tbl_id):
        self.where += f" rpv.pkg_vrs_id = {tbl_id}"
        self.join += f" join repositories.changelog rc on rc.pkg_vrs_id = rpv.pkg_vrs_id " \
                     f" join repositories.urgency ru on ru.urg_id = rc.urg_id"

    def run(self, pkg):
        self.create_query(pkg.pkg_id)
        self.run_query()
        return self.tbl_dict
