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
                "_1": re.sub('\"{', '', self.df_get_name(str(pdf["_1"][0]))).replace("'", "").strip(),
                "_2": self.df_get_name(str(pdf["_2"][0])).replace("'", "").strip(),
                "_3": self.df_get_name(str(pdf["_3"][0])).replace("'", "").strip(),
                "_4": self.df_get_name(str(pdf["_4"][0])).replace("'", "").strip(),
                "_5": self.df_get_name(str(pdf["_5"][0])).replace("'", "").strip(),
                "_6": self.df_get_name(str(pdf["_6"][0])).replace("'", "").strip(),
                "_7": self.df_get_name(str(pdf["_7"][0])).replace("'", "").strip(),
                "_8": self.df_get_name(str(pdf["_8"][0])).replace("'", "").strip(),
                "_9": self.df_get_name(str(pdf["_9"][0])).replace("'", "").strip(),
                "_10": self.df_get_name(str(pdf["_10"][0])).replace("'", "").strip(),
                "_11": self.df_get_name(str(pdf["_11"][0])).replace("'", "").strip(),
                "_12": self.df_get_name(str(pdf["_12"][0])).replace("'", "").strip(),
                "_13": self.df_get_name(str(pdf["_13"][0])).replace("'", "").strip(),
                "_14": self.df_get_name(str(pdf["_14"][0])).replace("'", "").strip(),
                "_15": self.df_get_name(str(pdf["_15"][0])).replace("'", "").strip(),
                "_16": self.df_get_name(str(pdf["_16"][0])).replace("'", "").strip(),
                "_17": self.df_get_name(str(pdf["_17"][0])).replace("'", "").strip(),
                "_18": self.df_get_name(str(pdf["_18"][0])).replace("'", "").strip(),
                "_19": self.df_get_name(str(pdf["_19"][0])).replace("'", "").strip(),
                "_20": self.df_get_name(str(pdf["_20"][0])).replace("'", "").strip(),
                "_21": self.df_get_name(str(pdf["_21"][0])).replace("'", "").strip(),
                "_22": self.df_get_name(str(pdf["_22"][0])).replace("'", "").strip(),
                "_23": self.df_get_name(str(pdf["_23"][0])).replace("'", "").strip(),
                "_24": self.df_get_name(str(pdf["_24"][0])).replace("'", "").strip(),
                "_25": self.df_get_name(str(pdf["_25"][0])).replace("'", "").strip(),
                "_26": self.df_get_name(str(pdf["_26"][0])).replace("'", "").strip(),
                "_27": self.df_get_name(str(pdf["_27"][0])).replace("'", "").strip(),
                "_28": self.df_get_name(str(pdf["_28"][0])).replace("'", "").strip(),
                "_29": self.df_get_name(str(pdf["_29"][0])).replace("'", "").strip(),
                "_30": self.df_get_name(str(pdf["_30"][0])).replace("'", "").strip(),
                "_31": self.df_get_name(str(pdf["_31"][0])).replace("'", "").strip(),
                "_32": self.df_get_name(str(pdf["_32"][0])).replace("'", "").strip(),
                "_33": self.df_get_name(str(pdf["_33"][0])).replace("'", "").strip(),
                "_34": self.df_get_name(str(pdf["_34"][0])).replace("'", "").strip(),
                "_35": self.df_get_name(str(pdf["_35"][0])).replace("'", "").strip(),
                "_36": self.df_get_name(str(pdf["_36"][0])).replace("'", "").strip(),
                "_37": self.df_get_name(str(pdf["_37"][0])).replace("'", "").strip(),
                "_38": self.df_get_name(str(pdf["_38"][0])).replace("'", "").strip(),
                "_39": self.df_get_name(str(pdf["_39"][0])).replace("'", "").strip(),
                "_40": self.df_get_name(str(pdf["_40"][0])).replace("'", "").strip(),
                "_41": self.df_get_name(str(pdf["_41"][0])).replace("'", "").strip(),
                "_42": self.df_get_name(str(pdf["_42"][0])).replace("'", "").strip(),
                "_43": self.df_get_name(str(pdf["_43"][0])).replace("'", "").strip(),
                "_44": self.df_get_name(str(pdf["_44"][0])).replace("'", "").strip(),
                "_45": self.df_get_name(str(pdf["_45"][0])).replace("'", "").strip(),
                "_46": self.df_get_name(str(pdf["_46"][0])).replace("'", "").strip(),
                "_47": self.df_get_name(str(pdf["_47"][0])).replace("'", "").strip(),
                "_48": self.df_get_name(str(pdf["_48"][0])).replace("'", "").strip(),
                "_49": self.df_get_name(str(pdf["_49"][0])).replace("'", "").strip(),
                "_50": self.df_get_name(str(pdf["_50"][0])).replace("'", "").strip(),
                "_51": self.df_get_name(str(pdf["_51"][0])).replace("'", "").strip(),
                "_52": self.df_get_name(str(pdf["_52"][0])).replace("'", "").strip(),
                "_53": self.df_get_name(str(pdf["_53"][0])).replace("'", "").strip(),
                "_54": self.df_get_name(str(pdf["_54"][0])).replace("'", "").strip(),
                "_55": self.df_get_name(str(pdf["_55"][0])).replace("'", "").strip(),
                "_56": self.df_get_name(str(pdf["_56"][0])).replace("'", "").strip(),
                "_57": self.df_get_name(str(pdf["_57"][0])).replace("'", "").strip(),
                "_58": self.df_get_name(str(pdf["_58"][0])).replace("'", "").strip(),
                "_59": self.df_get_name(str(pdf["_59"][0])).replace("'", "").strip(),
                "_60": self.df_get_name(str(pdf["_60"][0])).replace("'", "").strip(),
                "_61": re.sub('\}"', '', self.df_get_name(str(pdf["_61"][0]))).replace("'", "").strip()
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




