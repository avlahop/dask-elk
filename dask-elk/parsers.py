class DocumentParser(object):

    @classmethod
    def parse_documents(self, documents, meta):
        """

        :param pandas.DataFrame documents:
        :param pandas.DataFrame meta:
        :return: the document formated
        """
        meta_columns = meta.columns
        documents_columns = documents.columns

        missing_columns = self.find_missing_columns(meta,
                                                    documents)
        extra_columns = [column for column in documents_columns if
                         column not in meta_columns]

        for column_name, dtype in missing_columns.iteritems():
            documents[column_name] = None
            documents[column_name] = documents[column_name].astype(dtype)

        columns_to_cast = [column_name for column_name in meta_columns if
                           column_name not in missing_columns]

        for column_name in columns_to_cast:
            documents[column_name] = documents[column_name].astype(
                meta.dtypes[column_name])

        if extra_columns:
            documents = documents.drop(labels=extra_columns, axis=1)

        return documents

    @staticmethod
    def find_missing_columns(meta, documents):
        """
        :param pandas.DataFrame meta:
        :param pandas.DataFrame documents:
        :return:
        :rtype: dict[str, np.dtypes]
        """
        meta_columns = meta.columns
        documents_columns = documents.columns

        diff = [column_name for column_name in meta_columns if
                column_name not in documents_columns]

        missing_columns = {}
        for column_name in diff:
            missing_columns[column_name] = meta.dtypes[column_name]

        return missing_columns
