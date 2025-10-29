import logging  # Будем вести лог
import time
from datetime import datetime, timedelta  # Дата и время
import pandas as pd
from pytz import timezone  # Работаем с временнОй зоной
import asyncio
import websockets
import json
import logging
import zlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FkVikingMoney:
    data_money = None
    balance = 0.0
    equity = 0.0
    money = 0.0
    unrealized_profit = 0.0

    def __init__(self, _data):
        self.data_money = _data


class FkVikingPosition:
    data_position = None
    security = None
    symbol = None
    equity = 0.0
    balance = 0.0
    price_open = 0.0
    avg_price = 0.0
    current_price = 0.0
    unrealized_profit = 0.0

    def __init__(self, _data):
        self.security = _data
        self.symbol = _data


class FkVikingPortfolio:
    data_portfolio = None
    r_id = None
    p_id = None
    email = None
    securities = []
    positions = []
    equity = 0.0
    balance = 0.0
    unrealized_profit = 0.0

    def __init__(self, r_id, p_id, email):
        self.r_id = r_id
        self.p_id = p_id
        self.email = email


class FkVikingPy:
    def __init__(self, login: str, token: str, r_id: str, p_id: str, role: str, sec_type: str):
        #connection
        self.url = "wss://bot.fkviking.com/ws"
        self.login = login
        self.token = token
        self.role = role
        self.r_id = r_id
        self.p_id = p_id
        self.sec_type = sec_type
        self.sec_type_id = -1
        self.is_authenticated = False
        self.websocket = None
        #
        self.running = True
        self.reconnect_interval = 5
        self.data = None
        self.data_connect = None
        self.data_sec_types = None
        self.data_portfolios = None
        self.data_positions = None
        self.data_portfolio = None
        self.data_securities = None
        #
        self.portfolios = []
        self.positions = []
        self.money = None
        #
        self.listen_task = None
        self.loop = asyncio.get_event_loop()
        #
        self.aval_securities = []
        #

    def json_get_list_portfolios(self):
        result = {
            "type": "available_portfolio_list.subscribe",
            "data": {},
            "eid": "qwerty"
        }
        return result

    def json_get_aval_securities(self):
        result = {
            "type": "robot.get_securities",
            "data": {
                "r_id": self.r_id
            },
            "eid": "qwerty"
        }
        return result

    def json_find_aval_security(self, _symbol):
        result = {
            "type": "robot.find_security",
            "data": {
                "r_id": self.r_id,
                "key": _symbol
            },
            "eid": "qwerty"
        }
        return result

    def json_add_symbol(self, _symbol):
        result = {
            "type": "data_conn.add_symbol",
            "data":
                {
                    "sec_type": self.sec_type_id,
                    "name": _symbol,
                    "symbol": _symbol
                },
            "eid": "q"
        }
        return result

    def json_get_sec_types(self):
        #Запрос доступных sec_type
        result = {
            "type": "get_sec_types",
            "eid": "qwerty"
        }
        return result

    def json_get_portfolio(self, _r_id, _p_id):
        result = {
            "type": "portfolio.subscribe",
            "data": {
                "r_id": _r_id,
                "p_id": _p_id
            },
            "eid": "qwerty"
        }
        return result

    def json_get_authenticate(self):
        result = {
            "type": "authorization_key",
            "data":
                {
                    "email": self.login,
                    "key": self.token,
                    "role": self.role,
                    "group": 0,
                    "compress": True
                },
            "eid": "qwe"
        }
        return result

    def json_get_create_portofolio_symbol(self, security, _symbol, _board):
        # sec_type 67108864, => OKEX
        # sec_type 4, => MOEX
        result = {
            "type": "robot.add_portfolio",
            "data": {
                "r_id": self.r_id,
                "portfolio": {
                    "k": 0,
                    "k1": 0,
                    "k2": 0,
                    "tp": 1,
                    "pos": 0,
                    "to0": False,
                    "name": security.sec_key,
                    "color": "#FFFFFF",
                    "delta": 0,
                    "lim_b": 0,
                    "lim_s": 0,
                    "quote": False,
                    "timer": 1,
                    "v_max": 1,
                    "v_min": -1,
                    "_buy_v": 10,
                    "_pos_v": 1000,
                    "opened": 0,
                    "re_buy": False,
                    "use_tt": False,
                    "v_in_l": 1,
                    "v_in_r": 1,
                    "_buy_en": False,
                    "_l_b_en": False,
                    "_l_s_en": False,
                    "_pos_en": False,
                    "_sell_v": 10,
                    "comment": "",
                    "overlay": 0,
                    "percent": 100,
                    "re_sell": False,
                    "v_out_l": 1,
                    "v_out_r": 1,
                    "_l_b_val": 10,
                    "_l_s_val": 10,
                    "_sell_en": False,
                    "decimals": 4,
                    "disabled": False,
                    "_buy_time": 5,
                    "_l_b_stop": False,
                    "_l_b_time": 10,
                    "_l_s_stop": False,
                    "_l_s_time": 10,
                    "_pos_time": 5,
                    #"timetable": [
                    #    {
                    #        "begin": 36000,
                    #        "end": 50400,
                    #        "auto_close": False,
                    #        "auto_to_market": True,
                    #        "auto_to0": False
                    #    },
                    #    {
                    #        "begin": 50580,
                    #        "end": 67020,
                    #        "auto_close": False,
                    #        "auto_to_market": True,
                    #        "auto_to0": False
                    #    }
                    #],
                    "_sell_time": 5,
                    "mkt_volume": 100,
                    "price_type": 0,
                    "securities": {
                        security.sec_key: {
                            "k": 0,
                            "mm": False,
                            "sl": 0,
                            "te": True,
                            "tp": 1,
                            "pos": 0,
                            "sle": False,
                            "k_sl": 0,
                            "count": 1,
                            "ratio": 1,
                            "timer": 60,
                            "on_buy": 1,
                            "sec_key": security.sec_key,
                            "sec_board": _board,
                            "decimals": 4,
                            "depth_ob": 1,
                            "is_first": True,
                            "leverage": 1,
                            "ob_c_p_t": 1,
                            "ob_t_p_t": 0,
                            "sec_type": self.sec_type_id,
                            "comission": 0,
                            "ban_period": 1,
                            "count_type": 0,
                            "ratio_sign": 0,
                            "ratio_type": 0,
                            "client_code": "virtual",
                            "move_limits": False,
                            "fin_res_mult": 1,
                            "mc_level_to0": 0,
                            "move_limits1": False,
                            "count_formula": "return 1;",
                            "comission_sign": 1,
                            "mc_level_close": 0,
                            "sec_key_subscr": security.sec_key_subscr,
                            "max_trans_musec": 1000000,
                            "ratio_b_formula": "return 1;",
                            "ratio_s_formula": "return 1;",
                            "percent_of_quantity": 100
                        }
                    },
                    "type_trade": 0,
                    "_fin_res_en": False,
                    "ext_field1_": "return 0;",
                    "ext_field2_": "return 0;",
                    "first_delta": 0,
                    "hedge_after": 1,
                    "n_perc_fill": 0,
                    "price_check": 10,
                    "_fin_res_abs": 1000,
                    "_fin_res_val": 10,
                    "custom_trade": False,
                    "equal_prices": False,
                    "ext_formulas": False,
                    "simply_first": False,
                    "_fin_res_stop": False,
                    "_fin_res_time": 60,
                    "cur_day_month": 7,
                    "portfolio_num": 0,
                    "trade_formula": "return 0;",
                    "virtual_0_pos": False,
                    "max_not_hedged": 1,
                    "portfolio_type": 0,
                    "_max_running_en": False,
                    "_too_much_n_h_en": False,
                    "opened_comission": 0,
                    "move_limits1_date": -1,
                    "always_limits_timer": False,
                    "_max_running_percent": 70
                }
            },
            "eid": "qwerty"
        }
        return result

    async def connect(self):
        try:
            self.websocket = await websockets.connect(self.url)
            print(f"Подключен к {self.url}")
            return True
        except Exception as e:
            print(f"Ошибка подключения: {e}")
            return False

    async def connect_with_retry(self):
        while self.running:
            try:
                self.websocket = await websockets.connect(self.url)
                print(f"Подключен к {self.url}")
                return True
            except Exception as e:
                print(f"Ошибка подключения: {e}")
                print(f"Повторная попытка через {self.reconnect_interval} секунд...")
                await asyncio.sleep(self.reconnect_interval)
        return False

    async def send_message(self, message):
        str_json = json.dumps(message)
        bytes_str = bytes(str_json, encoding="utf-8")
        if await self.is_connected():
            try:
                await self.websocket.send(zlib.compress(bytes_str))
            except Exception as e:
                print(f"Ошибка отправки: {e}")

    async def receive_message(self):
        if await self.is_connected():
            try:
                message = await self.websocket.recv()
                decompressed = self.decompress_message(message)
                print(f"Получено: {decompressed}")
                return decompressed
            except websockets.exceptions.ConnectionClosed:
                print("Соединение закрыто сервером")
                return None
            except Exception as e:
                print(f"Ошибка получения: {e}")
                return None

    async def listen(self):
        """Постоянно слушает сообщения от сервера"""
        try:
            async for message in self.websocket:
                print(f"Получено: {message}")
                # Здесь можно добавить обработку различных типов сообщений
                decompressed = self.decompress_message(message)
                await self.handle_message(decompressed)
        except websockets.exceptions.ConnectionClosed:
            print("Соединение закрыто")
        except Exception as e:
            print(f"Ошибка в listen: {e}")

    async def handle_message(self, data):
        """Обработка полученных сообщений"""
        message_type = data.get("type")

        if message_type == "ping":
            await self.send_message({"type": "pong", "data": "pong"})
        elif message_type == "notification":
            print(f"Уведомление: {data.get('message')}")
        else:
            print(f"Неизвестный тип сообщения: {message_type}")

    async def close(self):
        if await self.is_connected():
            await self.websocket.close()
        print("Соединение закрыто")

    async def get_money(self):
        _data = None
        if await self.is_connected():
            try:
                await self.send_message(self.json_get_portfolio(self.r_id, self.p_id))
                _data = await self.receive_message()
            except Exception as e:
                print(f"Ошибка отправки: {e}")

        return _data

    def set_positions(self, _portfolios):
        _positions = []
        for portfolio in self.portfolios:
            for position in portfolio.positions:
                _positions.append(position)
        return _positions

    async def get_positions(self):
        if await self.is_connected():
            try:
                await self.send_message(self.json_get_portfolio(self.r_id, self.p_id))
                self.data = await self.receive_message()
            except Exception as e:
                print(f"Ошибка отправки: {e}")
        return self.positions

    async def get_portfolio(self, portfolio):
        r_id = portfolio[0]
        p_id = portfolio[1]
        email = portfolio[2]
        portfolio_obj = FkVikingPortfolio(r_id, p_id, email)
        _data = None
        if await self.is_connected():
            try:
                await self.send_message(self.json_get_portfolio(r_id, p_id))
                portfolio_obj.data_portfolio = await self.receive_message()
                #
                json_data = json.loads(portfolio_obj.data_portfolio)
                securities = json_data["data"]["value"]["securities"]
                for security in securities:
                    portfolio_obj.securities.append(security)
                    portfolio_obj.positions.append(FkVikingPosition(security))
            except Exception as e:
                print(f"Ошибка отправки: {e}")
        return portfolio_obj

    async def create_portfolio_symbol(self, security):
        result = None
        if await self.is_connected():
            try:
                await self.send_message(self.json_get_create_portofolio_symbol(security))
                result = await self.receive_message()
                #
            except KeyboardInterrupt:
                print("Получен сигнал прерывания")
        return result

    async def add_symbol_listen(self, symbol):
        result = None
        if await self.is_connected():
            try:
                await self.send_message(self.json_add_symbol(symbol))
                result = await self.receive_message()
                #
            except KeyboardInterrupt:
                print("Получен сигнал прерывания")
        return result

    async def update_aval_securities(self):
        await self.send_message(self.json_get_aval_securities())
        self.data_securities = await self.receive_message()
        self.get_aval_securities()

    async def delete_portfolio_symbol(self, security):
        result = None
        if await self.is_connected():
            try:
                await self.send_message(self.json_get_create_portofolio_symbol(security))
                result = await self.receive_message()
                #
            except Exception as e:
                print(f"Ошибка отправки: {e}")
        return result

    async def get_aval_security(self, symbol):
        result = None
        if await self.is_connected():
            try:
                await self.send_message(self.json_find_aval_security(symbol))
                result = await self.receive_message()
                #
            except Exception as e:
                print(f"Ошибка отправки: {e}")
        return result

    def find_aval_security(self, _data, _symbol):
        json_data = json.loads(_data)
        _list = json_data["data"]["portfolios"]
        if len(_list) > 0:
            return True
        return False

    def create_market_order(self, class_code="", security_code="", order_type="", quantity=1, comments=""):
        done = False
        _data = asyncio.run(self.get_aval_security(security_code))
        done = self.find_aval_security(_data, security_code)
        #for security in self.aval_securities:
        #
        #    if security.sec_key == security_code:
        #        _data = asyncio.run(self.create_portfolio_symbol(security))
        #        done = True
        if not done:
            _data = asyncio.run(self.add_symbol_listen(security_code))
            asyncio.run(self.add_symbol_listen(security_code))
            asyncio.run(self.update_aval_securities())
        str1 = ""
        return done

    def get_sec_type_id_by_name(self, sec_type_name):
        json_data = json.loads(self.data_sec_types)
        _list = json_data["data"]["sec_types"]
        for element in _list:
            if element["value"] == sec_type_name:
                return int(element["id"])
        return 4  #MOEX_FOND

    def get_aval_securities(self):
        json_data = json.loads(self.data_securities)
        _list = json_data["data"]["securities"]
        for element in _list:
            self.aval_securities.append(element)

    def decompress_message(self, message):
        """Декомпрессия сообщения"""
        try:
            return zlib.decompress(message).decode('utf-8')
        except:
            return message.decode('utf-8')

    async def run(self):
        await self.is_connected()
        try:
            #
            await self.send_message(self.json_get_sec_types())
            self.data_sec_types = await self.receive_message()
            self.sec_type_id = self.get_sec_type_id_by_name(self.sec_type)
            time.sleep(1)
            #
            await self.is_connected()
            #
            await self.send_message(self.json_get_list_portfolios())
            self.data_portfolios = await self.receive_message()
            json_data = json.loads(self.data_portfolios)
            portfolios_add = json_data["data"]["portfolios_add"]
            for portfolio in portfolios_add:
                self.portfolios.append(await self.get_portfolio(portfolio))
            self.positions = self.set_positions(self.portfolios)
            self.money = FkVikingMoney(self.get_money())
            time.sleep(1)
            #
            #await self.send_message(self.json_get_aval_securities())
            #self.data_securities = await self.receive_message()
            #self.get_aval_securities()
            #time.sleep(5)
            str1 = ""
        except Exception as e:
            print(f"Ошибка отправки: {e}")
        finally:
            if self.listen_task is not None:
                self.listen_task.cancel()
            await self.websocket.close()

    async def is_connected(self):
        need_reconnect = False
        if self.websocket is None:
            if await self.connect():
                #self.listen_task = asyncio.create_task(self.listen())
                try:
                    # Отправляем несколько сообщений
                    await self.send_message(self.json_get_authenticate())
                    self.data_connect = await self.receive_message()

                except Exception as e:
                    print(f"Ошибка отправки: {e}")
                return True
            else:
                return False
        elif self.websocket.state != 1:
            if await self.connect():
                #self.listen_task = asyncio.create_task(self.listen())
                try:
                    # Отправляем несколько сообщений
                    await self.send_message(self.json_get_authenticate())
                    self.data_connect = await self.receive_message()
                except Exception as e:
                    print(f"Ошибка отправки: {e}")
                return True
            else:
                return False
        else:
            need_reconnect = False
        return True

    def get_candles_from_provider(self, _board, _sym, start_date, tf, next_bar_open_utc=None):
        str1 = ""
        df = pd.DataFrame()
        return df

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.listen_task is not None:
            self.listen_task.cancel()
        self.websocket.close()

    def __del__(self):
        if self.listen_task is not None:
            self.listen_task.cancel()
        self.websocket.close()
