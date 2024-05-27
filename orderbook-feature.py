import pandas as pd
import math
import timeit
import os

mid_type = ''

def cal_mid_price(gr_bid_level, gr_ask_level):
    level = 5

    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0].price
        bid_top_level_qty = gr_bid_level.iloc[0].quantity
        ask_top_price = gr_ask_level.iloc[0].price
        ask_top_level_qty = gr_ask_level.iloc[0].quantity
        mid_price = (bid_top_price + ask_top_price) * 0.5

        return (mid_price, bid_top_price, ask_top_price, bid_top_level_qty, ask_top_level_qty)

    else:
        print('Error: serious cal_mid_price')
        return (-1, -1, -2, -1, -1)

def get_diff_count_units(diff):
    _count_1 = _count_0 = _units_traded_1 = _units_traded_0 = 0
    _price_1 = _price_0 = 0

    diff_len = len(diff)
    if diff_len == 1:
        row = diff.iloc[0]
        if row['type'] == 1:
            _count_1 = row['count']
            _units_traded_1 = row['units_traded']
            _price_1 = row['price']
        else:
            _count_0 = row['count']
            _units_traded_0 = row['units_traded']
            _price_0 = row['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

    elif diff_len == 2:
        row_1 = diff.iloc[1]
        row_0 = diff.iloc[0]
        _count_1 = row_1['count']
        _count_0 = row_0['count']

        _units_traded_1 = row_1['units_traded']
        _units_traded_0 = row_0['units_traded']

        _price_1 = row_1['price']
        _price_0 = row_0['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

def live_cal_book_i_v1(param, gr_bid_level, gr_ask_level, diff, var, mid):
    mid_price = mid

    ratio = param[0]; level = param[1]; interval = param[2]

    _flag = var['_flag']

    if _flag: #skipping first line
        var['_flag'] = False
        return 0.0

    quant_v_bid = gr_bid_level.quantity**ratio
    price_v_bid = gr_bid_level.price * quant_v_bid

    quant_v_ask = gr_ask_level.quantity**ratio
    price_v_ask = gr_ask_level.price * quant_v_ask

    askQty = quant_v_ask.values.sum()
    bidPx = price_v_bid.values.sum()
    bidQty = quant_v_bid.values.sum()
    askPx = price_v_ask.values.sum()
    bid_ask_spread = interval

    book_price = 0 #because of warning, divisible by 0
    if bidQty > 0 and askQty > 0:
        book_price = (((askQty*bidPx)/bidQty) + ((bidQty*askPx)/askQty)) / (bidQty+askQty)

    indicator_value = (book_price - mid_price) / bid_ask_spread

    return indicator_value

def init_indicator_var(indicator, param):
    return {'_flag': True}

def faster_calc_indicators(raw_fn):
    # 절대 경로 지정
    file_path = raw_fn

    # 파일 존재 여부 확인
    if os.path.exists(file_path):
        print("File exists.")
        try:
            # CSV 파일 읽기
            df = pd.read_csv(file_path, header=None)

            # 열 이름 수동 설정
            df.columns = ['price', 'quantity', 'type', 'timestamp']

            start_time = timeit.default_timer()

            group_o = df.groupby('timestamp')
            group_t = df.groupby('timestamp')

            delay = timeit.default_timer() - start_time
            print('df loading delay: %.2fs' % delay)

            level_1 = 2
            level_2 = 5

            print('param levels', level_1, level_2)

            book_imbalance_params = [(0.2,level_1,1),(0.2,level_2,1)]
            book_delta_params = [(0.2,level_1,1),(0.2,level_1,5),(0.2,level_1,15), (0.2,level_2,1),(0.2,level_2,5),(0.2,level_2,15)]
            trade_indicator_params = [(0.2,level_1,1),(0.2,level_1,5),(0.2,level_1,15)]

            variables = {}
            _dict = {}
            _dict_indicators = {}

            for p in book_imbalance_params:
                indicator = 'BI'
                _dict.update( {(indicator, p): []} )
                _dict_var = init_indicator_var(indicator, p)
                variables.update({(indicator, p): _dict_var})

            for p in book_delta_params:
                indicator = 'BDv1'
                _dict.update( {(indicator, p): []} )
                _dict_var = init_indicator_var(indicator, p)
                variables.update({(indicator, p): _dict_var})

                indicator = 'BDv2'
                _dict.update( {(indicator, p): []} )
                _dict_var = init_indicator_var(indicator, p)
                variables.update({(indicator, p): _dict_var})

                indicator = 'BDv3'
                _dict.update( {(indicator, p): []} )
                _dict_var = init_indicator_var(indicator, p)
                variables.update({(indicator, p): _dict_var})

            for p in trade_indicator_params:

                indicator = 'TIv1'
                _dict.update( {(indicator, p): []} )
                _dict_var = init_indicator_var(indicator, p)
                variables.update({(indicator, p): _dict_var})

                indicator = 'TIv2'
                _dict.update( {(indicator, p): []} )
                _dict_var = init_indicator_var(indicator, p)
                variables.update({(indicator, p): _dict_var})

            _timestamp = []
            _mid_price = []

            seq = 0
            print('total groups:', len(group_o.size().index), len(group_t.size().index))

            # 그룹의 길이가 같은지 확인
            if len(group_o) != len(group_t):
                print(f"Warning: group_o and group_t have different lengths: {len(group_o)} != {len(group_t)}")

            for (timestamp_o, gr_o), (timestamp_t, gr_t) in zip(group_o, group_t):

                # 타임스탬프가 일치하는지 확인
                if timestamp_o != timestamp_t:
                    print(f"Warning: Mismatched timestamps {timestamp_o} and {timestamp_t}")
                    continue

                if gr_o.empty or gr_t.empty:
                    print('Warning: group is empty')
                    continue

                timestamp = gr_o.iloc[0]['timestamp']

                gr_bid_level = gr_o[gr_o['type'] == 0]
                gr_ask_level = gr_o[gr_o['type'] == 1]
                diff = get_diff_count_units(gr_t)

                mid_price, bid, ask, bid_qty, ask_qty = cal_mid_price(gr_bid_level, gr_ask_level)

                if bid >= ask:
                    seq += 1
                    continue

                _timestamp.append(timestamp)
                _mid_price.append(mid_price)

                _dict_group = {}
                for (indicator, p) in _dict.keys():
                    level = p[1]
                    if level not in _dict_group:

                        orig_level = level
                        level = min(level, len(gr_bid_level), len(gr_ask_level))

                        _dict_group[level] = (gr_bid_level.head(level), gr_ask_level.head(level))

                    p1 = ()
                    if len(p) == 3:
                        p1 = (p[0], level, p[2])
                    elif len(p) == 4:
                        p1 = (p[0], level, p[2], p[3])

                    _i = live_cal_book_i_v1(p1, _dict_group[level][0], _dict_group[level][1], diff, variables[(indicator,p)], mid_price)
                    _dict[(indicator,p)].append(_i)

                for (indicator, p) in _dict.keys():
                    if len(p) == 3:
                        col_name = '%s-%s-%s-%s' % (indicator, p[0], p[1], p[2])
                    elif len(p) == 4:
                        col_name = '%s-%s-%s-%s-%s' % (indicator, p[0], p[1], p[2], p[3])

                    _dict_indicators[col_name] = _dict[(indicator,p)]

                _dict_indicators['timestamp'] = _timestamp
                _dict_indicators['mid_price'] = _mid_price

                seq += 1

            fn = '2024-04-28-bithumb-btc-feature.csv'
            pd.DataFrame(_dict_indicators).to_csv(fn)

        except FileNotFoundError as e:
            print(f"FileNotFoundError: {e}")
    else:
        print("File does not exist. Please check the path.")

raw_fn = 'C:/Users/yoony/book-2024-04-28-exchange-market-BTC.csv'
faster_calc_indicators(raw_fn)
