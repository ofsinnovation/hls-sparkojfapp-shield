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
        pdf = raw_df.toPandas()
        pdf.rename(
            columns={
                "_1": re.sub('\"{', '', self.df_get_name(str(pdf["_1"][0]))).replace("'", ""),
                "_2": self.df_get_name(str(pdf["_2"][0])).replace("'", ""),
                "_3": self.df_get_name(str(pdf["_3"][0])).replace("'", ""),
                "_4": self.df_get_name(str(pdf["_4"][0])).replace("'", ""),
                "_5": self.df_get_name(str(pdf["_5"][0])).replace("'", ""),
                "_6": self.df_get_name(str(pdf["_6"][0])).replace("'", ""),
                "_7": self.df_get_name(str(pdf["_7"][0])).replace("'", ""),
                "_8": self.df_get_name(str(pdf["_8"][0])).replace("'", ""),
                "_9": self.df_get_name(str(pdf["_9"][0])).replace("'", ""),
                "_10": self.df_get_name(str(pdf["_10"][0])).replace("'", ""),
                "_11": self.df_get_name(str(pdf["_11"][0])).replace("'", ""),
                "_12": self.df_get_name(str(pdf["_12"][0])).replace("'", ""),
                "_13": self.df_get_name(str(pdf["_13"][0])).replace("'", ""),
                "_14": self.df_get_name(str(pdf["_14"][0])).replace("'", ""),
                "_15": self.df_get_name(str(pdf["_15"][0])).replace("'", ""),
                "_16": self.df_get_name(str(pdf["_16"][0])).replace("'", ""),
                "_17": self.df_get_name(str(pdf["_17"][0])).replace("'", ""),
                "_18": self.df_get_name(str(pdf["_18"][0])).replace("'", ""),
                "_19": self.df_get_name(str(pdf["_19"][0])).replace("'", ""),
                "_20": self.df_get_name(str(pdf["_20"][0])).replace("'", ""),
                "_21": self.df_get_name(str(pdf["_21"][0])).replace("'", ""),
                "_22": self.df_get_name(str(pdf["_22"][0])).replace("'", ""),
                "_23": self.df_get_name(str(pdf["_23"][0])).replace("'", ""),
                "_24": self.df_get_name(str(pdf["_24"][0])).replace("'", ""),
                "_25": self.df_get_name(str(pdf["_25"][0])).replace("'", ""),
                "_26": self.df_get_name(str(pdf["_26"][0])).replace("'", ""),
                "_27": self.df_get_name(str(pdf["_27"][0])).replace("'", ""),
                "_28": self.df_get_name(str(pdf["_28"][0])).replace("'", ""),
                "_29": self.df_get_name(str(pdf["_29"][0])).replace("'", ""),
                "_30": self.df_get_name(str(pdf["_30"][0])).replace("'", ""),
                "_31": self.df_get_name(str(pdf["_31"][0])).replace("'", ""),
                "_32": self.df_get_name(str(pdf["_32"][0])).replace("'", ""),
                "_33": self.df_get_name(str(pdf["_33"][0])).replace("'", ""),
                "_34": self.df_get_name(str(pdf["_34"][0])).replace("'", ""),
                "_35": self.df_get_name(str(pdf["_35"][0])).replace("'", ""),
                "_36": self.df_get_name(str(pdf["_36"][0])).replace("'", ""),
                "_37": self.df_get_name(str(pdf["_37"][0])).replace("'", ""),
                "_38": self.df_get_name(str(pdf["_38"][0])).replace("'", ""),
                "_39": self.df_get_name(str(pdf["_39"][0])).replace("'", ""),
                "_40": self.df_get_name(str(pdf["_40"][0])).replace("'", ""),
                "_41": self.df_get_name(str(pdf["_41"][0])).replace("'", ""),
                "_42": self.df_get_name(str(pdf["_42"][0])).replace("'", ""),
                "_43": self.df_get_name(str(pdf["_43"][0])).replace("'", ""),
                "_44": self.df_get_name(str(pdf["_44"][0])).replace("'", ""),
                "_45": self.df_get_name(str(pdf["_45"][0])).replace("'", ""),
                "_46": self.df_get_name(str(pdf["_46"][0])).replace("'", ""),
                "_47": self.df_get_name(str(pdf["_47"][0])).replace("'", ""),
                "_48": self.df_get_name(str(pdf["_48"][0])).replace("'", ""),
                "_49": self.df_get_name(str(pdf["_49"][0])).replace("'", ""),
                "_50": self.df_get_name(str(pdf["_50"][0])).replace("'", ""),
                "_51": self.df_get_name(str(pdf["_51"][0])).replace("'", ""),
                "_52": self.df_get_name(str(pdf["_52"][0])).replace("'", ""),
                "_53": self.df_get_name(str(pdf["_53"][0])).replace("'", ""),
                "_54": self.df_get_name(str(pdf["_54"][0])).replace("'", ""),
                "_55": self.df_get_name(str(pdf["_55"][0])).replace("'", ""),
                "_56": self.df_get_name(str(pdf["_56"][0])).replace("'", ""),
                "_57": self.df_get_name(str(pdf["_57"][0])).replace("'", ""),
                "_58": self.df_get_name(str(pdf["_58"][0])).replace("'", ""),
                "_59": self.df_get_name(str(pdf["_59"][0])).replace("'", ""),
                "_60": self.df_get_name(str(pdf["_60"][0])).replace("'", ""),
                "_61": re.sub('\}"', '', self.df_get_name(str(pdf["_61"][0]))).replace("'", "")
            },
            inplace=True
        )

        ## Pandas HACK
        ## Should I do self.pdf or just pdf
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




