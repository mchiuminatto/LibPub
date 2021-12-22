class DFMapper:

    def __init__(self):
        self.TGT_FIELD_MAPING = {"PositionId": "position_id",
                                 "Instrument": "inst_id",
                                 "Bar Size": "bar_size_id",
                                 "Direction": "dir_id",
                                 "Open date": "open_date",
                                 "Close date": "close_date",
                                 "Open price": "open_price",
                                 "Close price": "close_price",
                                 "Open volume": "open_volume",
                                 "Close volume": "close_volume",
                                 "Size": "size",
                                 "max_retrace_price": "max_retrace_price",
                                 "max_retrace_date": "max_retrace_date",
                                 "max_retrace_pips": "max_retrace_pips",
                                 "max_advance_price": "max_advance_price",
                                 "max_advance_date": "max_advance_date",
                                 "max_advance_pips": "max_advance_pips",
                                 "Periods": "periods",
                                 "Profit": "profit",
                                 "net_pft_pip": "net_profit",
                                 "acc_profit_pip": "acc_profit_pip"}


    def Transform(self, df):
        _col_list  = list(dict.fromkeys(self.TGT_FIELD_MAPING))
        return df[_col_list].rename(columns = self.TGT_FIELD_MAPING)
