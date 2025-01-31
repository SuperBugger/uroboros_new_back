from ..query_commands.base_query import BaseApi
import logging


class AssemblyApi(BaseApi):
    def __init__(self, db_helper):
        super().__init__()
        self._db_helper = db_helper
        self.name_col = ["assm_id", "assm_date_created", "assm_desc",
                         "prj_id", "assm_version"]
        self.table_name = "repositories.assembly"
        self.fields = "assm_id, assm_date_created, assm_desc, prj_id, assm_version"

    def create_query(self, tbl_id):
        if tbl_id is not None:
            self.where = f"prj_id = {tbl_id}"
        else:
            self._error("prj_id can't be null")

    def run(self, prj_id):
        try:
            self.create_query(prj_id)
            self.run_query()
            return self.tbl_dict
        except Exception as e:
            logging.error(f"Failed to run assembly query: {e}")
            self._error(e)
