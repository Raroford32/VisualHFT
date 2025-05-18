using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using TardisDev;
using VisualHFT.Commons.Helpers;
using VisualHFT.Commons.Interfaces;
using VisualHFT.Commons.Model;
using VisualHFT.Commons.PluginManager;
using VisualHFT.Commons.Pools;
using VisualHFT.Enums;
using VisualHFT.PluginManager;
using VisualHFT.UserSettings;

namespace MarketConnectors.BitStamp
{
    public class BitStampPlugin : BasePluginDataRetriever, IDataRetrieverTestable
    {
        private bool _disposed = false; // to track whether the object has been disposed

        private PlugInSettings _settings;
        private TardisAPI _tardisApi;
        private Dictionary<string, VisualHFT.Model.OrderBook> _localOrderBooks = new Dictionary<string, VisualHFT.Model.OrderBook>();
        private HelperCustomQueue<IBinanceEventOrderBook> _eventBuffers;
        private HelperCustomQueue<IBinanceTrade> _tradesBuffers;
        private int pingFailedAttempts = 0;
        private System.Timers.Timer _timerPing;

        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private readonly CustomObjectPool<VisualHFT.Model.Trade> tradePool = new CustomObjectPool<VisualHFT.Model.Trade>();//pool of Trade objects

        private Dictionary<string, VisualHFT.Model.Order> _localUserOrders = new Dictionary<string, VisualHFT.Model.Order>();

        public override string Name { get; set; } = "BitStamp Plugin";
        public override string Version { get; set; } = "1.0.0";
        public override string Description { get; set; } = "Connects to BitStamp websockets.";
        public override string Author { get; set; } = "VisualHFT";
        public override ISetting Settings { get => _settings; set => _settings = (PlugInSettings)value; }
        public override Action CloseSettingWindow { get; set; }

        public BitStampPlugin()
        {
            SetReconnectionAction(InternalStartAsync);
            log.Info($"{this.Name} has been loaded.");
        }
        ~BitStampPlugin()
        {
            Dispose(false);
        }

                    RaiseOnDataReceived(GetProviderModel(eSESSIONSTATUS.DISCONNECTED_FAILED));
                }
                else
                {
                    await HandleConnectionLost(_error, ex);
                }
            }
        }

        private async Task InternalStartAsync()
        {
            await ClearAsync();
            await SetupClientsAsync();

            _eventBuffers = new HelperCustomQueue<IBinanceEventOrderBook>($"<IBinanceEventOrderBook>_{this.Name}", eventBuffers_onReadAction, eventBuffers_onErrorAction);

            //Pause QUEUES until we get the snapshots ready
            await InitializePingTimerAsync();

            log.Info($"Plugin has successfully started.");
            Status = ePluginStatus.STOPPING;
            log.Info($"{this.Name} is stopping.");

            await ClearAsync();
                foreach (var lob in _localOrderBooks)
                {
                    lob.Value?.Dispose();
                }
                _localOrderBooks.Clear();
            }
        }

        private async Task SetupClientsAsync()
        {
            _tardisApi = new TardisAPI(_settings.ApiKey);
        }

        private async Task InitializeTradesAsync()
        {
            log.Info($"{this.Name}: sending WS Trades Subscription to all symbols ");
                foreach (var trade in trades)
                {
                    _tradesBuffers.Add(trade);
                }
            }
        }

            foreach (var symbol in GetAllNonNormalizedSymbols())
            {
                var orderBookUpdates = await _tardisApi.GetOrderBookUpdatesAsync(symbol);
                {
                    _localOrderBooks.Add(normalizedSymbol, null);
                }
                log.Info($"{this.Name}: Getting snapshot {normalizedSymbol} level 2");

        {
            var symbol = GetNormalizedSymbol(eventData.Symbol);
            UpdateOrderBook(eventData, symbol);
        }

        private void eventBuffers_onErrorAction(Exception ex)
        {
            var _error = $"Will reconnect. Unhandled error in the Market Data Queue: {ex.Message}";

            log.Error(_error, ex);
            Task.Run(async () => await HandleConnectionLost(_error, ex));
        }

        private void tradesBuffers_onReadAction(IBinanceTrade eventData)
        {
            var _symbol = GetNormalizedSymbol(eventData.Symbol);
            var trade = tradePool.Get();
            trade.Price = eventData.Price;
            trade.Size = eventData.Quantity;
            trade.Symbol = _symbol;
            trade.Timestamp = eventData.TradeTime.ToLocalTime();
            trade.ProviderId = _settings.Provider.ProviderID;
            RaiseOnDataReceived(trade);
            tradePool.Return(trade);
        }

        private void tradesBuffers_onErrorAction(Exception ex)
        {
            Task.Run(async () => await HandleConnectionLost(_error, ex));
        }

        private void UpdateOrderBook(IBinanceEventOrderBook lob_update, string normalizedSymbol)
        {
            if (!_localOrderBooks.ContainsKey(normalizedSymbol))
                return;

                return;

            if (lob_update.FirstUpdateId > local_lob.Sequence &&
                lob_update.FirstUpdateId != local_lob.Sequence + 1)
                throw new Exception("Detected sequence gap.");

            foreach (var item in lob_update.Bids)
            {
                if (item.Quantity != 0)
                {
                    local_lob.AddOrUpdateLevel(new DeltaBookItem()
                    {
                        IsBid = true,
                        LocalTimeStamp = DateTime.Now,
                        ServerTimeStamp = ts,
                        Symbol = normalizedSymbol
                    });
            }
            foreach (var item in lob_update.Asks)
            {
                if (item.Quantity != 0)
                        MDUpdateAction = eMDUpdateAction.Delete,
                        Price = (double)item.Price,
                        IsBid = false,
                        LocalTimeStamp = DateTime.Now,
                        ServerTimeStamp = ts,
                        Symbol = normalizedSymbol
                    });
            }
            local_lob.Sequence = lob_update.LastUpdateId;

            RaiseOnDataReceived(local_lob);
        }

        private async Task DoPingAsync()
        {
            try
            {
                if (Status == ePluginStatus.STOPPED || Status == ePluginStatus.STOPPING || Status == ePluginStatus.STOPPED_FAILED)
                    return;

                bool isConnected = _tardisApi != null;
                if (!isConnected)
                    var timeLapseInMicroseconds = DateTime.Now.Subtract(ini).TotalMicroseconds;

                    pingFailedAttempts = 0;

                    RaiseOnDataReceived(GetProviderModel(eSESSIONSTATUS.CONNECTED));
                }
                else
                {
                    throw new Exception("Ping failed, result was null.");
            var identifiedPriceDecimalPlaces = RecognizeDecimalPlacesAutomatically(data.Asks.Select(x => x.Price));

            var lob = new VisualHFT.Model.OrderBook(GetNormalizedSymbol(data.Symbol), identifiedPriceDecimalPlaces, _settings.DepthLevels);
            lob.ProviderID = _settings.Provider.ProviderID;
            lob.ProviderName = _settings.Provider.ProviderName;
            lob.SizeDecimalPlaces = RecognizeDecimalPlacesAutomatically(data.Asks.Select(x => x.Quantity));

            var _asks = new List<VisualHFT.Model.BookItem>();
                    IsBid = false,
                    Price = (double)x.Price,
                    Size = (double)x.Quantity,
                    SizeDecimalPlaces = lob.SizeDecimalPlaces,
                    ProviderID = lob.ProviderID,
                });
            });
            lob.LoadData(
                _asks.OrderBy(x => x.Price).Take(_settings.DepthLevels),
                _bids.OrderByDescending(x => x.Price).Take(_settings.DepthLevels)
            );
            return lob;
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true;
                if (disposing)
                {
                    _timerPing?.Dispose();

                    _eventBuffers?.Dispose();
                    _tradesBuffers?.Dispose();

                    if (_localOrderBooks != null)
                    {
                        foreach (var lob in _localOrderBooks)
                        {
                            lob.Value?.Dispose();
                        }
                        _localOrderBooks.Clear();
                    }

                    base.Dispose();
                }
            }
        }

        protected override void LoadSettings()
        {
            _settings = LoadFromUserSettings<PlugInSettings>();
            if (_settings == null)
            {
                InitializeDefaultSettings();
            }
            if (_settings.Provider == null)
            {
                _settings.Provider = new VisualHFT.Model.Provider() { ProviderID = 1, ProviderName = "BitStamp" };
            }
            ParseSymbols(string.Join(',', _settings.Symbols.ToArray()));
        }

        protected override void SaveSettings()
        {
            SaveToUserSettings(_settings);
                _settings.ApiKey = viewModel.ApiKey;
                _settings.UpdateIntervalMs = viewModel.UpdateIntervalMs;
                _settings.Provider = new VisualHFT.Model.Provider() { ProviderID = viewModel.ProviderId, ProviderName = viewModel.ProviderName };
                _settings.Symbols = viewModel.Symbols;
                _settings.IsNonUS = viewModel.IsNonUS;
            localModel.Symbol = snapshotModel.Symbol;
            localModel.Bids = snapshotModel.Bids.Select(x => new BinanceOrderBookEntry() { Price = x.Price.ToDecimal(), Quantity = x.Size.ToDecimal() }).ToList();
            localModel.Asks = snapshotModel.Asks.Select(x => new BinanceOrderBookEntry() { Price = x.Price.ToDecimal(), Quantity = x.Size.ToDecimal() }).ToList();
            _settings.DepthLevels = snapshotModel.MaxDepth;

            var symbol = snapshotModel.Symbol;

            if (!_localOrderBooks.ContainsKey(symbol))
            {
                _localOrderBooks.Add(symbol, ToOrderBookModel(localModel));
            }
            _localOrderBooks[symbol] = ToOrderBookModel(localModel);

            RaiseOnDataReceived(_localOrderBooks[symbol]);
            var symbol = bidDeltaModel?.FirstOrDefault()?.Symbol;
            if (symbol == null)
                symbol = askDeltaModel?.FirstOrDefault()?.Symbol;
            var localModel = new BinanceEventOrderBook();
            localModel.Bids = bidDeltaModel?.Select(x => new BinanceOrderBookEntry() { Price = x.Price.ToDecimal(), Quantity = x.Size.ToDecimal() }).ToList();
            string _file = "";
            if (scenario == eTestingPrivateMessageScenario.SCENARIO_1)
                _file = "PrivateMessages_Scenario1.json";
            else if (scenario == eTestingPrivateMessageScenario.SCENARIO_2)
                _file = "PrivateMessages_Scenario2.json";
            else if (scenario == eTestingPrivateMessageScenario.SCENARIO_4)
                _file = "PrivateMessages_Scenario4.json";
            else if (scenario == eTestingPrivateMessageScenario.SCENARIO_5)
                _file = "PrivateMessages_Scenario5.json";
            else if (scenario == eTestingPrivateMessageScenario.SCENARIO_6)
                _file = "PrivateMessages_Scenario6.json";
            else if (scenario == eTestingPrivateMessageScenario.SCENARIO_7)
                _file = "PrivateMessages_Scenario9.json";
                throw new Exception("Messages collected for this scenario don't look good.");
            }
            else if (scenario == eTestingPrivateMessageScenario.SCENARIO_10)
            {
                _file = "PrivateMessages_Scenario10.json";
                throw new Exception("Messages were not collected for this scenario.");
            }

            var dataEvents = new List<BinanceStreamOrderUpdate>();
            var jsonArray = JArray.Parse(jsonString);
            foreach (var jsonObject in jsonArray)
            {
                JToken dataToken = jsonObject["data"];
                string dataJsonString = dataToken.ToString();

            if (!modelList.Any())
                throw new Exception("No data was found in the json file.");
            foreach (var item in modelList)
            foreach (var item in modelList)
            {
                VisualHFT.Model.Order localuserOrder;
                if (!dicOrders.ContainsKey(item.ClientOrderId))
                {
                    localuserOrder = new VisualHFT.Model.Order();
                    localuserOrder.OrderID = item.Id;
                    localuserOrder.Currency = GetNormalizedSymbol(item.Symbol);
                    localuserOrder.CreationTimeStamp = item.CreateTime;
                    localuserOrder.ProviderName = _settings.Provider.ProviderName;
                    localuserOrder.CreationTimeStamp = item.CreateTime;
                    localuserOrder.Quantity = (double)item.Quantity;
                    localuserOrder.PricePlaced = (double)item.Price;
                    {
                        localuserOrder.TimeInForce = eORDERTIMEINFORCE.IOC;
                    }
                    }
                    if (item.Type == SpotOrderType.Market)
                    {
                    }
                    else if (item.Side == OrderSide.Sell)
                    {
                        localuserOrder.Side = eORDERSIDE.Sell;
                    }

                    dicOrders.Add(item.ClientOrderId, localuserOrder);
                }
                else
                {
                if (item.Status == OrderStatus.New || item.Status == OrderStatus.PendingNew)
                {
                    if (item.Side == OrderSide.Buy)
                    {
                        localuserOrder.CreationTimeStamp = item.CreateTime;
                        localuserOrder.PricePlaced = (double)item.Price;
                    localuserOrder.Status = eORDERSTATUS.NEW;
                }
                if (item.Status == OrderStatus.Filled)
                {
                    localuserOrder.BestAsk = (double)item.Price;
                    localuserOrder.BestBid = (double)item.Price;
                    localuserOrder.FilledQuantity = (double)(item.QuantityFilled);
                    localuserOrder.Status = eORDERSTATUS.FILLED;
                }
                if (item.Status == OrderStatus.Canceled)
                {
                    localuserOrder.Status = eORDERSTATUS.CANCELED;
                }

                if (item.Status == OrderStatus.Rejected)
                {
                    localuserOrder.Status = eORDERSTATUS.REJECTED;
                }

                if (item.Status == OrderStatus.PartiallyFilled)
                {
                    localuserOrder.BestAsk = (double)item.Price;
                    localuserOrder.BestBid = (double)item.Price;
                    localuserOrder.Status = eORDERSTATUS.PARTIALFILLED;
                }

                if (!string.IsNullOrEmpty(item.OriginalClientOrderId) && item.OriginalClientOrderId != item.ClientOrderId)
                {
                    if (dicOrders.TryGetValue(item.OriginalClientOrderId, out var originalOrder))
