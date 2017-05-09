import re

class ParserDecorator(object):
    def __init__(self):
        pass

    def general_parser(self, x):
        len_val = len(x)
        token_index = x.index(": ")
        val_str = x[(token_index + 1):(len_val)]
        return val_str

    def time_parser(self, x):
        len_val = len(x)
        token_index = x.index(": ")
        val_str = x[(token_index + 4):(len_val - 1)]
        return val_str

    def last_element_parser(self, x):
        len_val = len(x)
        token_index = x.index(": ")
        val_str = x[(token_index + 1):(len_val - 1)]
        return val_str

    def df_column_rename(self, raw_df):
        #print("============> RAW DF ============>")
        #print(raw_df)
        #pandas_df = raw_df.toPandas()
        #print("converted pandas df in the decorator =====>")
        #print(pandas_df)
        #raw_df = self.raw_df

        pdf = raw_df.toPandas()
        print("=======> Checking out the get name hack")
        print(self.df_get_name(str(pdf["_1"][0])))

        df = raw_df.withColumnRenamed("_1", "imuanklemagyaxis") \
            .withColumnRenamed("_2", "heartrate") \
            .withColumnRenamed("_3", "imuchestgyrozaxis") \
            .withColumnRenamed("_4", "imuchestacc6gyaxis") \
            .withColumnRenamed("_5", "rowid") \
            .withColumnRenamed("_6", "datetime") \
            .withColumnRenamed("_7", "peopleid") \
            .withColumnRenamed("_8", "imuhandacc16gyaxis") \
            .withColumnRenamed("_9", "imuchestacc6gxaxis") \
            .withColumnRenamed("_10", "imuchestacc6gzaxis") \
            .withColumnRenamed("_11", "imuchesttemp") \
            .withColumnRenamed("_12", "imuankletemp") \
            .withColumnRenamed("_13", "imuchestmagyaxis") \
            .withColumnRenamed("_14", "imuankleacc16gzaxis") \
            .withColumnRenamed("_15", "imuankleacc16gxaxis") \
            .withColumnRenamed("_16", "imuhandacc6gzaxis") \
            .withColumnRenamed("_17", "imuchestgyroxaxis") \
            .withColumnRenamed("_18", "imuchestmagxaxis") \
            .withColumnRenamed("_19", "sourceid") \
            .withColumnRenamed("_20", "seqno") \
            .withColumnRenamed("_21", "imuankleacc6gyaxis") \
            .withColumnRenamed("_22", "imuhandori4") \
            .withColumnRenamed("_23", "imuhandgyroxaxis") \
            .withColumnRenamed("_24", "imuhandori2") \
            .withColumnRenamed("_25", "imuhandori1") \
            .withColumnRenamed("_26", "imuankleacc16gyaxis") \
            .withColumnRenamed("_27", "imuhandmagxaxis") \
            .withColumnRenamed("_28", "imuhandacc16gxaxis") \
            .withColumnRenamed("_29", "imuchestori2") \
            .withColumnRenamed("_30", "imuchestori3") \
            .withColumnRenamed("_31", "imuchestori1") \
            .withColumnRenamed("_32", "imuchestori4") \
            .withColumnRenamed("_33", "imuhandmagzaxis") \
            .withColumnRenamed("_34", "imuhandtemp") \
            .withColumnRenamed("_35", "isactive") \
            .withColumnRenamed("_36", "imuankleacc6gzaxis") \
            .withColumnRenamed("_37", "imuankleacc6gxaxis") \
            .withColumnRenamed("_38", "imuhandgyroyaxis") \
            .withColumnRenamed("_39", "imuankleori3") \
            .withColumnRenamed("_40", "imuhandacc6gyaxis") \
            .withColumnRenamed("_41", "imuankleori4") \
            .withColumnRenamed("_42", "imuanklegyroxaxis") \
            .withColumnRenamed("_43", "activityid") \
            .withColumnRenamed("_44", "imuanklegyrozaxis") \
            .withColumnRenamed("_45", "imuankleori1") \
            .withColumnRenamed("_46", "imuankleori2") \
            .withColumnRenamed("_47", "imuchestacc16gzaxis") \
            .withColumnRenamed("_48", "imuhandacc6gxaxis") \
            .withColumnRenamed("_49", "imuhandgyrozaxis") \
            .withColumnRenamed("_50", "imuhandacc16gzaxis") \
            .withColumnRenamed("_51", "imuanklegyroyaxis") \
            .withColumnRenamed("_52", "imuchestacc16gyaxis") \
            .withColumnRenamed("_53", "imuanklemagxaxis") \
            .withColumnRenamed("_54", "imuanklemagzaxis") \
            .withColumnRenamed("_55", "datasetfileid") \
            .withColumnRenamed("_56", "imuchestacc16gxaxis") \
            .withColumnRenamed("_57", "imuhandmagyaxis") \
            .withColumnRenamed("_58", "imuchestgyroyaxis") \
            .withColumnRenamed("_59", "imuchestmagzaxis") \
            .withColumnRenamed("_60", "imuhandori3") \
            .withColumnRenamed("_61", "datasetid")
        ## Pandas HACK
        ## Should I do self.pdf or just pdf
        pdf = df.toPandas()
        # print(pdf)
        return pdf

    def data_parameter_extraction(self, pdf, current_stream_index, column_index, column_name, type_parser):

        data_column = pdf.iloc[:, [column_index]]
        raw_data = data_column.iloc[current_stream_index][column_name]

        if (type_parser == 'general_parser'):
            data = self.general_parser(raw_data)
            return (data)

        if (type_parser == 'int_parser'):
            data = self.general_parser(raw_data)
            data = str(int(data.encode('ascii', 'ignore')))
            return (data)

        if (type_parser == 'float_parser'):
            data = self.general_parser(raw_data)
            data = str(float(data.encode('ascii', 'ignore')))
            return (data)

        if (type_parser == 'time_parser'):
            data = self.time_parser(raw_data)
            return (data)

        if (type_parser == 'last_element_parser'):
            data = self.last_element_parser(raw_data)
            return (data)

    def lreplace(pattern, sub, string):
        """
        Replaces 'pattern' in 'string' with 'sub' if 'pattern' starts 'string'.
        """
        return re.sub('^%s' % pattern, sub, string)

    def rreplace(pattern, sub, string):
        """
        Replaces 'pattern' in 'string' with 'sub' if 'pattern' ends 'string'.
        """
        return re.sub('%s$' % pattern, sub, string)

    def df_get_value(self, x):
        # the split the data frame value which contains the columnname:value
        y = x.split(':')
        return y[1]

    def df_get_name(self, x):
        y = x.split(':')
        return y[0]



