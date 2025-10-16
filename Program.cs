using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
        IConfiguration config = builder.Build();

        string? apiKey = config.GetValue<string>("BinanceApiSettings:ApiKey");
        string? apiSecret = config.GetValue<string>("BinanceApiSettings:ApiSecret");
        string? connectionString = config.GetConnectionString("PostgresDb");

        if (string.IsNullOrEmpty(apiKey) || string.IsNullOrEmpty(apiSecret) || apiKey.Contains("AQUI_VA") || string.IsNullOrEmpty(connectionString))
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Error: Revise las credenciales de la API y la cadena de conexión en appsettings.json.");
            Console.ResetColor();
            Console.ReadKey();
            return;
        }

        Console.WriteLine("=============================================");
        Console.WriteLine("  Bienvenido al ETL de Binance a PostgreSQL  ");
        Console.WriteLine("=============================================");
        Console.WriteLine("\nConfiguración cargada correctamente.");
        int ProcessOrdersCount = 1;
        int postDays = 1;
        while (ProcessOrdersCount > 0)
        {
            // CAMBIO: Se obtiene el rango de fechas automáticamente desde la base de datos.
            var dateRange = await GetDateRangeAsync(connectionString, postDays);
            if (dateRange == null)
            {
                Console.ReadKey();
                return;
            }

            var result = ConvertDateToTimestamps(dateRange.Value.startDate, dateRange.Value.endDate);

            ProcessOrdersCount = await FetchAndProcessOrdersAsync(apiKey, apiSecret, connectionString, result.startTimestamp, result.endTimestamp);
            DateTime yesterday = DateTime.UtcNow.AddDays(-1).Date;
            if (ProcessOrdersCount == 0
                && (dateRange.Value.endDate.Day != yesterday.Day ||
                dateRange.Value.endDate.Month != yesterday.Month ||
                dateRange.Value.endDate.Year != yesterday.Year))
            {
                postDays++;
                ProcessOrdersCount = 1; // Forzar un nuevo ciclo para intentar con más días
            }
            else
            {
                postDays = 1; // Reiniciar postDays si se procesaron órdenes
            }
        }

        // =======================================================================
        // --- INICIO: NUEVA FUNCIONALIDAD DE ACTUALIZACIÓN DE KYC (REFACORIZADO) ---
        // =======================================================================

        // Se registra el codificador para poder leer archivos excel, es requerido por la librería.
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

        // Se crea una instancia del nuevo procesador y se ejecuta el proceso de actualización.
        var kycProcessor = new KycUpdateProcessor();
        await kycProcessor.ProcessAsync(config, connectionString);

        // =======================================================================
        // --- FIN: NUEVA FUNCIONALIDAD ---
        // =======================================================================

        Console.WriteLine("\nProceso finalizado. Presione cualquier tecla para salir.");
        Console.ReadKey();
    }

    // CAMBIO: Nueva función para obtener el rango de fechas automáticamente.
    private static async Task<(DateTime startDate, DateTime endDate)?> GetDateRangeAsync(string connectionString, int postDays)
    {
        Console.WriteLine("\nDeterminando rango de fechas para la extracción...");
        DateTime startDate;
        DateTime endDate;

        await using var conn = new NpgsqlConnection(connectionString);
        try
        {
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(@"SELECT MAX(""CreateTime"") FROM ""public"".""Orders""", conn);
            var result = await cmd.ExecuteScalarAsync();

            if (result == null || result == DBNull.Value)
            {
                // Caso 1: La base de datos está vacía.
                Console.WriteLine("Base de datos vacía. Calculando rango inicial.");
                startDate = DateTime.UtcNow.AddMonths(-11).Date;
                endDate = startDate.AddDays(postDays);
            }
            else
            {
                // Caso 2: Se encontró una fecha máxima.
                startDate = ((DateTime)result).AddSeconds(1).ToUniversalTime();
                endDate = startDate.AddDays(postDays);
                Console.WriteLine($"Última orden encontrada el: {startDate:yyyy-MM-dd}. Calculando siguiente rango.");
            }

            // Validación: La fecha final no puede ser hoy ni futura.
            DateTime yesterday = DateTime.UtcNow.AddDays(-1).Date;
            if (endDate > yesterday)
            {
                endDate = yesterday;
            }

            if (startDate > endDate)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Los datos ya están actualizados hasta el día de ayer. No hay nada que procesar.");
                Console.ResetColor();
                return null;
            }

            return (startDate, endDate);
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error al consultar la base de datos para obtener el rango de fechas: {ex.Message}");
            Console.ResetColor();
            return null;
        }
    }

    private static (DateTime startDate, DateTime endDate, long startTimestamp, long endTimestamp) ConvertDateToTimestamps(DateTime userStartDate, DateTime userEndDate)
    {
        DateTime startDate = DateTime.SpecifyKind(userStartDate, DateTimeKind.Utc);
        DateTime endDate = DateTime.SpecifyKind(userEndDate, DateTimeKind.Utc);

        long startTimestamp = new DateTimeOffset(startDate).ToUnixTimeMilliseconds();
        long endTimestamp = new DateTimeOffset(endDate).ToUnixTimeMilliseconds();

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"\nPerfecto. Se procesarán los datos en el siguiente rango:");
        Console.ResetColor();
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine($"   - Desde (Timestamp): {startTimestamp}  ({startDate:u})");
        Console.WriteLine($"   - Hasta (Timestamp): {endTimestamp}  ({endDate:u})");
        Console.ResetColor();

        return (startDate, endDate, startTimestamp, endTimestamp);
    }

    // El resto del código permanece igual...
    private static async Task<int> FetchAndProcessOrdersAsync(string apiKey, string apiSecret, string connectionString, long startTimestamp, long endTimestamp)
    {
        var allOrders = await GetOrderListAsync(apiKey, apiSecret, startTimestamp, endTimestamp);

        if (allOrders == null || !allOrders.Any())
        {
            Console.WriteLine("No se encontraron órdenes para procesar o hubo un error al obtener la lista.");
            return 0;
        }

        Console.WriteLine($"\nIniciando procesamiento de {allOrders.Count} órdenes...");
        int counter = 0;
        foreach (var orderSummary in allOrders)
        {
            counter++;
            string? orderNumber = orderSummary.GetProperty("orderNumber").GetString();
            if (string.IsNullOrEmpty(orderNumber)) continue;

            Console.WriteLine($"Procesando {counter}/{allOrders.Count}: OrderNumber {orderNumber}");

            var orderDetailElement = await FetchOrderDetailAsync(apiKey, apiSecret, orderNumber);
            if (orderDetailElement == null)
            {
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Console.WriteLine($"  -> No se pudo obtener el detalle para la orden {orderNumber}. Se omite.");
                Console.ResetColor();
                continue;
            }

            var orderDetail = orderDetailElement.Value;
            string? takerUserNo = orderDetail.TryGetProperty("takerUserNo", out var uno) ? uno.GetString() : null;
            if (string.IsNullOrEmpty(takerUserNo))
            {
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Console.WriteLine($"  -> No se encontró takerUserNo en el detalle para la orden {orderNumber}. Se omite.");
                Console.ResetColor();
                continue;
            }

            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var transaction = await conn.BeginTransactionAsync();

            try
            {
                await SaveUserAsync(orderDetail, takerUserNo, conn);
                await SaveOrderAsync(orderSummary, conn);
                await SaveOrderDetailAsync(orderDetail, takerUserNo, conn);
                await SavePayMethodsAsync(orderDetail, conn);

                await transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"  -> Error al procesar la orden {orderNumber} en la base de datos: {ex.Message}");
                Console.ResetColor();
            }

            await Task.Delay(250);
        }
        return allOrders.Count;
    }

    private static async Task<List<JsonElement>?> GetOrderListAsync(string apiKey, string apiSecret, long startTimestamp, long endTimestamp)
    {
        const int rowsPerPage = 20;
        var allOrders = new List<JsonElement>();
        int currentPage = 1;
        int totalRecords = 0;

        do
        {
            const string baseUrl = "https://api.binance.com";
            const string endpoint = "/sapi/v1/c2c/orderMatch/listOrders";

            using var client = new HttpClient { BaseAddress = new Uri(baseUrl) };
            client.DefaultRequestHeaders.Add("X-MBX-APIKEY", apiKey);
            client.DefaultRequestHeaders.Add("clientType", "web");

            long requestTimestamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
            string queryStringToSign = $"timestamp={requestTimestamp}";
            string signature = GenerateSignature(queryStringToSign, apiSecret);
            string finalUrl = $"{endpoint}?{queryStringToSign}&signature={signature}";

            var requestBody = new { orderStatusList = new[] { 4, 6, 7 }, page = currentPage, rows = rowsPerPage, startDate = startTimestamp, endDate = endTimestamp };
            string jsonBody = JsonSerializer.Serialize(requestBody);
            var httpContent = new StringContent(jsonBody, Encoding.UTF8, "application/json");

            if (currentPage == 1)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("\nInformación de la petición inicial (lista de órdenes):");
                Console.WriteLine($"URL: {baseUrl}{finalUrl}");
                Console.WriteLine($"Body: {jsonBody}");
                Console.ResetColor();
            }

            try
            {
                HttpResponseMessage response = await client.PostAsync(finalUrl, httpContent);
                if (response.IsSuccessStatusCode)
                {
                    string responseBody = await response.Content.ReadAsStringAsync();
                    using var jsonDocument = JsonDocument.Parse(responseBody);
                    var root = jsonDocument.RootElement;
                    if (root.GetProperty("success").GetBoolean())
                    {
                        var data = root.GetProperty("data").EnumerateArray().ToList();
                        allOrders.AddRange(data.Select(el => el.Clone()));
                        if (currentPage == 1)
                        {
                            totalRecords = root.GetProperty("total").GetInt32();
                            if (totalRecords == 0) { Console.WriteLine("\nNo se encontraron órdenes para procesar."); return new List<JsonElement>(); }
                        }
                        double totalPages = Math.Ceiling((double)totalRecords / rowsPerPage);
                        Console.WriteLine($"Página de órdenes {currentPage} de {totalPages} obtenida. {allOrders.Count} de {totalRecords} órdenes recuperadas.");
                        currentPage++;
                    }
                    else { Console.WriteLine($"\nError en la respuesta de Binance: {root.GetProperty("message").GetString()}"); return null; }
                }
                else { Console.WriteLine($"\nError al llamar a la API: {response.StatusCode}"); return null; }
            }
            catch (Exception ex) { Console.WriteLine($"\nExcepción durante la petición: {ex.ToString()}"); return null; }
            if (allOrders.Count < totalRecords) { await Task.Delay(250); }
        } while (allOrders.Count < totalRecords && totalRecords > 0);

        return allOrders;
    }

    private static async Task<JsonElement?> FetchOrderDetailAsync(string apiKey, string apiSecret, string orderNumber)
    {
        const string baseUrl = "https://api.binance.com";
        const string endpoint = "/sapi/v1/c2c/orderMatch/getUserOrderDetail";

        using var client = new HttpClient { BaseAddress = new Uri(baseUrl) };
        client.DefaultRequestHeaders.Add("X-MBX-APIKEY", apiKey);
        client.DefaultRequestHeaders.Add("clientType", "web");

        long requestTimestamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
        string queryStringToSign = $"timestamp={requestTimestamp}";
        string signature = GenerateSignature(queryStringToSign, apiSecret);
        string finalUrl = $"{endpoint}?{queryStringToSign}&signature={signature}";

        var requestBody = new { adOrderNo = orderNumber };
        string jsonBody = JsonSerializer.Serialize(requestBody);
        var httpContent = new StringContent(jsonBody, Encoding.UTF8, "application/json");

        try
        {
            HttpResponseMessage response = await client.PostAsync(finalUrl, httpContent);
            if (response.IsSuccessStatusCode)
            {
                string responseBody = await response.Content.ReadAsStringAsync();
                var jsonDocument = JsonDocument.Parse(responseBody);
                if (jsonDocument.RootElement.GetProperty("success").GetBoolean())
                {
                    return jsonDocument.RootElement.GetProperty("data").Clone();
                }
            }
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"  -> Excepción al obtener detalle para {orderNumber}: {ex.Message}");
            Console.ResetColor();
        }
        return null;
    }

    private static async Task SaveUserAsync(JsonElement orderDetail, string takerUserNo, NpgsqlConnection conn)
    {
        await using var cmd = new NpgsqlCommand(@"
            INSERT INTO ""public"".""Users"" (""TakerUserNo"", ""Nickname"", ""FullName"", ""MobilePhone"", ""KycAvailable"", ""KycLevel"")
            VALUES (@TakerUserNo, @Nickname, @FullName, @MobilePhone, @KycAvailable, @KycLevel)
            ON CONFLICT (""TakerUserNo"") DO UPDATE SET
                ""Nickname"" = EXCLUDED.""Nickname"",
                ""FullName"" = EXCLUDED.""FullName"",
                ""MobilePhone"" = EXCLUDED.""MobilePhone"";
        ", conn);

        cmd.Parameters.AddWithValue("TakerUserNo", takerUserNo);
        cmd.Parameters.AddWithValue("Nickname", orderDetail.GetProperty("sellerNickname").GetString());
        cmd.Parameters.AddWithValue("FullName", orderDetail.GetProperty("sellerName").GetString());
        cmd.Parameters.AddWithValue("MobilePhone", orderDetail.GetProperty("sellerMobilePhone").GetString());
        cmd.Parameters.AddWithValue("KycAvailable", false);
        cmd.Parameters.AddWithValue("KycLevel", 1);

        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SaveOrderAsync(JsonElement orderSummary, NpgsqlConnection conn)
    {
        await using var cmd = new NpgsqlCommand(@"
            INSERT INTO ""public"".""Orders"" (
                ""OrderNumber"", ""AdvNo"", ""TradeType"", ""Asset"", ""Fiat"", ""FiatSymbol"", ""Amount"", ""TotalPrice"",
                ""OrderStatus"", ""CreateTime"", ""ConfirmPayEndTime"", ""NotifyPayEndTime"", ""SellerNickname"",
                ""BuyerNickname"", ""CommissionRate"", ""Commission"", ""CurrencyTicketSize"", ""AssetTicketSize"",
                ""PriceTicketSize"", ""ChatUnreadCount"", ""TakerCommissionRate"", ""TakerCommission"", ""TakerAmount"", ""AdditionalKycVerify""
            ) VALUES (
                @OrderNumber, @AdvNo, @TradeType, @Asset, @Fiat, @FiatSymbol, @Amount, @TotalPrice,
                @OrderStatus, @CreateTime, @ConfirmPayEndTime, @NotifyPayEndTime, @SellerNickname,
                @BuyerNickname, @CommissionRate, @Commission, @CurrencyTicketSize, @AssetTicketSize,
                @PriceTicketSize, @ChatUnreadCount, @TakerCommissionRate, @TakerCommission, @TakerAmount, @AdditionalKycVerify
            )
            ON CONFLICT (""OrderNumber"") DO UPDATE SET
                ""AdvNo"" = EXCLUDED.""AdvNo"", ""TradeType"" = EXCLUDED.""TradeType"", ""Asset"" = EXCLUDED.""Asset"",
                ""Fiat"" = EXCLUDED.""Fiat"", ""FiatSymbol"" = EXCLUDED.""FiatSymbol"", ""Amount"" = EXCLUDED.""Amount"",
                ""TotalPrice"" = EXCLUDED.""TotalPrice"", ""OrderStatus"" = EXCLUDED.""OrderStatus"", ""CreateTime"" = EXCLUDED.""CreateTime"",
                ""ConfirmPayEndTime"" = EXCLUDED.""ConfirmPayEndTime"", ""NotifyPayEndTime"" = EXCLUDED.""NotifyPayEndTime"",
                ""SellerNickname"" = EXCLUDED.""SellerNickname"", ""BuyerNickname"" = EXCLUDED.""BuyerNickname"",
                ""CommissionRate"" = EXCLUDED.""CommissionRate"", ""Commission"" = EXCLUDED.""Commission"",
                ""CurrencyTicketSize"" = EXCLUDED.""CurrencyTicketSize"", ""AssetTicketSize"" = EXCLUDED.""AssetTicketSize"",
                ""PriceTicketSize"" = EXCLUDED.""PriceTicketSize"", ""ChatUnreadCount"" = EXCLUDED.""ChatUnreadCount"",
                ""TakerCommissionRate"" = EXCLUDED.""TakerCommissionRate"", ""TakerCommission"" = EXCLUDED.""TakerCommission"",
                ""TakerAmount"" = EXCLUDED.""TakerAmount"", ""AdditionalKycVerify"" = EXCLUDED.""AdditionalKycVerify"";
        ", conn);

        cmd.Parameters.AddWithValue("OrderNumber", orderSummary.GetProperty("orderNumber").GetString());
        cmd.Parameters.AddWithValue("AdvNo", orderSummary.GetProperty("advNo").GetString());
        cmd.Parameters.AddWithValue("TradeType", orderSummary.GetProperty("tradeType").GetString());
        cmd.Parameters.AddWithValue("Asset", orderSummary.GetProperty("asset").GetString());
        cmd.Parameters.AddWithValue("Fiat", orderSummary.GetProperty("fiat").GetString());
        cmd.Parameters.AddWithValue("FiatSymbol", orderSummary.GetProperty("fiatSymbol").GetString());
        cmd.Parameters.AddWithValue("Amount", GetDecimalFromString(orderSummary, "amount"));
        cmd.Parameters.AddWithValue("TotalPrice", GetDecimalFromString(orderSummary, "totalPrice"));
        cmd.Parameters.AddWithValue("OrderStatus", orderSummary.GetProperty("orderStatus").GetInt32());
        cmd.Parameters.AddWithValue("CreateTime", DateTimeOffset.FromUnixTimeMilliseconds(orderSummary.GetProperty("createTime").GetInt64()).UtcDateTime);
        cmd.Parameters.AddWithValue("ConfirmPayEndTime", orderSummary.TryGetProperty("confirmPayEndTime", out var c) ? DateTimeOffset.FromUnixTimeMilliseconds(c.GetInt64()).UtcDateTime : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("NotifyPayEndTime", DateTimeOffset.FromUnixTimeMilliseconds(orderSummary.GetProperty("notifyPayEndTime").GetInt64()).UtcDateTime);
        cmd.Parameters.AddWithValue("SellerNickname", orderSummary.GetProperty("sellerNickname").GetString());
        cmd.Parameters.AddWithValue("BuyerNickname", orderSummary.GetProperty("buyerNickname").GetString());
        cmd.Parameters.AddWithValue("CommissionRate", GetDecimalFromString(orderSummary, "commissionRate"));
        cmd.Parameters.AddWithValue("Commission", GetDecimalFromString(orderSummary, "commission"));
        cmd.Parameters.AddWithValue("CurrencyTicketSize", GetDecimalFromString(orderSummary, "currencyTicketSize"));
        cmd.Parameters.AddWithValue("AssetTicketSize", GetDecimalFromString(orderSummary, "assetTicketSize"));
        cmd.Parameters.AddWithValue("PriceTicketSize", GetDecimalFromString(orderSummary, "priceTicketSize"));
        cmd.Parameters.AddWithValue("ChatUnreadCount", orderSummary.GetProperty("chatUnreadCount").GetInt32());
        cmd.Parameters.AddWithValue("TakerCommissionRate", GetDecimalFromString(orderSummary, "takerCommissionRate"));
        cmd.Parameters.AddWithValue("TakerCommission", GetDecimalFromString(orderSummary, "takerCommission"));
        cmd.Parameters.AddWithValue("TakerAmount", GetDecimalFromString(orderSummary, "takerAmount"));
        cmd.Parameters.AddWithValue("AdditionalKycVerify", orderSummary.GetProperty("additionalKycVerify").GetInt32());

        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SaveOrderDetailAsync(JsonElement orderDetail, string takerUserNo, NpgsqlConnection conn)
    {
        await using var cmd = new NpgsqlCommand(@"
            INSERT INTO ""public"".""OrderDetails"" (
                ""OrderNumber"", ""TakerUserNo"", ""AdvOrderNumber"", ""BuyerMobilePhone"", ""SellerMobilePhone"", ""BuyerNickname"", ""BuyerName"", ""SellerNickname"", ""SellerName"", ""TradeType"",
                ""PayType"", ""SelectedPayId"", ""OrderStatus"", ""Asset"", ""Amount"", ""Price"", ""TotalPrice"", ""FiatUnit"", ""IsComplaintAllowed"", ""ConfirmPayTimeout"",
                ""Remark"", ""CreateTime"", ""NotifyPayTime"", ""ConfirmPayTime"", ""NotifyPayEndTime"", ""ConfirmPayEndTime"", ""ExpectedPayTime"", ""ExpectedReleaseTime"",
                ""FiatSymbol"", ""CurrencyTicketSize"", ""AssetTicketSize"", ""PriceTicketSize"", ""NotifyPayedExpireMinute"", ""ConfirmPayedExpireMinute"", ""ClientType"",
                ""OnlineStatus"", ""MerchantNo"", ""Origin"", ""UnreadCount"", ""IconUrl"", ""AvgReleasePeriod"", ""AvgPayPeriod"", ""CommissionRate"", ""Commission"",
                ""TakerCommissionRate"", ""TakerCommission"", ""TakerAmount"", ""AdditionalKycVerify""
            ) VALUES (
                @OrderNumber, @TakerUserNo, @AdvOrderNumber, @BuyerMobilePhone, @SellerMobilePhone, @BuyerNickname, @BuyerName, @SellerNickname, @SellerName, @TradeType,
                @PayType, @SelectedPayId, @OrderStatus, @Asset, @Amount, @Price, @TotalPrice, @FiatUnit, @IsComplaintAllowed, @ConfirmPayTimeout,
                @Remark, @CreateTime, @NotifyPayTime, @ConfirmPayTime, @NotifyPayEndTime, @ConfirmPayEndTime, @ExpectedPayTime, @ExpectedReleaseTime,
                @FiatSymbol, @CurrencyTicketSize, @AssetTicketSize, @PriceTicketSize, @NotifyPayedExpireMinute, @ConfirmPayedExpireMinute, @ClientType,
                @OnlineStatus, @MerchantNo, @Origin, @UnreadCount, @IconUrl, @AvgReleasePeriod, @AvgPayPeriod, @CommissionRate, @Commission,
                @TakerCommissionRate, @TakerCommission, @TakerAmount, @AdditionalKycVerify
            )
            ON CONFLICT (""OrderNumber"") DO UPDATE SET
                ""TakerUserNo"" = EXCLUDED.""TakerUserNo"", ""AdvOrderNumber"" = EXCLUDED.""AdvOrderNumber"", ""BuyerMobilePhone"" = EXCLUDED.""BuyerMobilePhone"",
                ""SellerMobilePhone"" = EXCLUDED.""SellerMobilePhone"", ""BuyerNickname"" = EXCLUDED.""BuyerNickname"", ""BuyerName"" = EXCLUDED.""BuyerName"",
                ""SellerNickname"" = EXCLUDED.""SellerNickname"", ""SellerName"" = EXCLUDED.""SellerName"", ""TradeType"" = EXCLUDED.""TradeType"", ""PayType"" = EXCLUDED.""PayType"",
                ""SelectedPayId"" = EXCLUDED.""SelectedPayId"", ""OrderStatus"" = EXCLUDED.""OrderStatus"", ""Asset"" = EXCLUDED.""Asset"", ""Amount"" = EXCLUDED.""Amount"",
                ""Price"" = EXCLUDED.""Price"", ""TotalPrice"" = EXCLUDED.""TotalPrice"", ""FiatUnit"" = EXCLUDED.""FiatUnit"", ""IsComplaintAllowed"" = EXCLUDED.""IsComplaintAllowed"",
                ""ConfirmPayTimeout"" = EXCLUDED.""ConfirmPayTimeout"", ""Remark"" = EXCLUDED.""Remark"", ""CreateTime"" = EXCLUDED.""CreateTime"", ""NotifyPayTime"" = EXCLUDED.""NotifyPayTime"",
                ""ConfirmPayTime"" = EXCLUDED.""ConfirmPayTime"", ""NotifyPayEndTime"" = EXCLUDED.""NotifyPayEndTime"", ""ConfirmPayEndTime"" = EXCLUDED.""ConfirmPayEndTime"",
                ""ExpectedPayTime"" = EXCLUDED.""ExpectedPayTime"", ""ExpectedReleaseTime"" = EXCLUDED.""ExpectedReleaseTime"", ""FiatSymbol"" = EXCLUDED.""FiatSymbol"",
                ""CurrencyTicketSize"" = EXCLUDED.""CurrencyTicketSize"", ""AssetTicketSize"" = EXCLUDED.""AssetTicketSize"", ""PriceTicketSize"" = EXCLUDED.""PriceTicketSize"",
                ""NotifyPayedExpireMinute"" = EXCLUDED.""NotifyPayedExpireMinute"", ""ConfirmPayedExpireMinute"" = EXCLUDED.""ConfirmPayedExpireMinute"", ""ClientType"" = EXCLUDED.""ClientType"",
                ""OnlineStatus"" = EXCLUDED.""OnlineStatus"", ""MerchantNo"" = EXCLUDED.""MerchantNo"", ""Origin"" = EXCLUDED.""Origin"", ""UnreadCount"" = EXCLUDED.""UnreadCount"",
                ""IconUrl"" = EXCLUDED.""IconUrl"", ""AvgReleasePeriod"" = EXCLUDED.""AvgReleasePeriod"", ""AvgPayPeriod"" = EXCLUDED.""AvgPayPeriod"", ""CommissionRate"" = EXCLUDED.""CommissionRate"",
                ""Commission"" = EXCLUDED.""Commission"", ""TakerCommissionRate"" = EXCLUDED.""TakerCommissionRate"", ""TakerCommission"" = EXCLUDED.""TakerCommission"",
                ""TakerAmount"" = EXCLUDED.""TakerAmount"", ""AdditionalKycVerify"" = EXCLUDED.""AdditionalKycVerify"";
        ", conn);

        cmd.Parameters.AddWithValue("OrderNumber", orderDetail.GetProperty("orderNumber").GetString());
        cmd.Parameters.AddWithValue("TakerUserNo", takerUserNo);
        cmd.Parameters.AddWithValue("AdvOrderNumber", orderDetail.GetProperty("advOrderNumber").GetString());
        cmd.Parameters.AddWithValue("BuyerMobilePhone", orderDetail.GetProperty("buyerMobilePhone").GetString());
        cmd.Parameters.AddWithValue("SellerMobilePhone", orderDetail.GetProperty("sellerMobilePhone").GetString());
        cmd.Parameters.AddWithValue("BuyerNickname", orderDetail.GetProperty("buyerNickname").GetString());
        cmd.Parameters.AddWithValue("BuyerName", orderDetail.GetProperty("buyerName").GetString());
        cmd.Parameters.AddWithValue("SellerNickname", orderDetail.GetProperty("sellerNickname").GetString());
        cmd.Parameters.AddWithValue("SellerName", orderDetail.GetProperty("sellerName").GetString());
        cmd.Parameters.AddWithValue("TradeType", orderDetail.GetProperty("tradeType").GetString());
        cmd.Parameters.AddWithValue("PayType", orderDetail.GetProperty("payType").GetString());
        cmd.Parameters.AddWithValue("SelectedPayId", orderDetail.GetProperty("selectedPayId").GetInt64());
        cmd.Parameters.AddWithValue("OrderStatus", orderDetail.GetProperty("orderStatus").GetInt32());
        cmd.Parameters.AddWithValue("Asset", orderDetail.GetProperty("asset").GetString());
        cmd.Parameters.AddWithValue("Amount", GetDecimalFromString(orderDetail, "amount"));
        cmd.Parameters.AddWithValue("Price", GetDecimalFromString(orderDetail, "price"));
        cmd.Parameters.AddWithValue("TotalPrice", GetDecimalFromString(orderDetail, "totalPrice"));
        cmd.Parameters.AddWithValue("FiatUnit", orderDetail.GetProperty("fiatUnit").GetString());
        cmd.Parameters.AddWithValue("IsComplaintAllowed", orderDetail.GetProperty("isComplaintAllowed").GetBoolean());
        cmd.Parameters.AddWithValue("ConfirmPayTimeout", orderDetail.GetProperty("confirmPayTimeout").GetInt32());
        cmd.Parameters.AddWithValue("Remark", orderDetail.GetProperty("remark").GetString());
        cmd.Parameters.AddWithValue("CreateTime", DateTimeOffset.FromUnixTimeMilliseconds(orderDetail.GetProperty("createTime").GetInt64()).UtcDateTime);
        cmd.Parameters.AddWithValue("NotifyPayTime", orderDetail.TryGetProperty("NotifyPayTime", out var NotifyPayTime) ? DateTimeOffset.FromUnixTimeMilliseconds(NotifyPayTime.GetInt64()).UtcDateTime : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("ConfirmPayTime", orderDetail.TryGetProperty("ConfirmPayTime", out var ConfirmPayTime) ? DateTimeOffset.FromUnixTimeMilliseconds(ConfirmPayTime.GetInt64()).UtcDateTime : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("NotifyPayEndTime", DateTimeOffset.FromUnixTimeMilliseconds(orderDetail.GetProperty("notifyPayEndTime").GetInt64()).UtcDateTime);
        cmd.Parameters.AddWithValue("ConfirmPayEndTime", orderDetail.TryGetProperty("ConfirmPayEndTime", out var ConfirmPayEndTime) ? DateTimeOffset.FromUnixTimeMilliseconds(ConfirmPayEndTime.GetInt64()).UtcDateTime : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("ExpectedPayTime", DateTimeOffset.FromUnixTimeMilliseconds(orderDetail.GetProperty("expectedPayTime").GetInt64()).UtcDateTime);
        cmd.Parameters.AddWithValue("ExpectedReleaseTime", orderDetail.TryGetProperty("ExpectedReleaseTime", out var ExpectedReleaseTime) ? DateTimeOffset.FromUnixTimeMilliseconds(ExpectedReleaseTime.GetInt64()).UtcDateTime : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("FiatSymbol", orderDetail.GetProperty("fiatSymbol").GetString());
        cmd.Parameters.AddWithValue("CurrencyTicketSize", GetDecimalFromString(orderDetail, "currencyTicketSize"));
        cmd.Parameters.AddWithValue("AssetTicketSize", GetDecimalFromString(orderDetail, "assetTicketSize"));
        cmd.Parameters.AddWithValue("PriceTicketSize", GetDecimalFromString(orderDetail, "priceTicketSize"));
        cmd.Parameters.AddWithValue("NotifyPayedExpireMinute", orderDetail.GetProperty("notifyPayedExpireMinute").GetInt32());
        cmd.Parameters.AddWithValue("ConfirmPayedExpireMinute", orderDetail.GetProperty("confirmPayedExpireMinute").GetInt32());
        cmd.Parameters.AddWithValue("ClientType", orderDetail.GetProperty("clientType").GetString());
        cmd.Parameters.AddWithValue("OnlineStatus", orderDetail.GetProperty("onlineStatus").GetString());
        cmd.Parameters.AddWithValue("MerchantNo", orderDetail.TryGetProperty("merchantNo", out var m) ? m.GetString() : (object)DBNull.Value);
        cmd.Parameters.AddWithValue("Origin", orderDetail.GetProperty("origin").GetString());
        cmd.Parameters.AddWithValue("UnreadCount", orderDetail.GetProperty("unreadCount").GetInt32());
        cmd.Parameters.AddWithValue("IconUrl", orderDetail.GetProperty("iconUrl").GetString());
        cmd.Parameters.AddWithValue("AvgReleasePeriod", orderDetail.GetProperty("avgReleasePeriod").GetInt32());
        cmd.Parameters.AddWithValue("AvgPayPeriod", orderDetail.GetProperty("avgPayPeriod").GetInt32());
        cmd.Parameters.AddWithValue("CommissionRate", GetDecimalFromString(orderDetail, "commissionRate"));
        cmd.Parameters.AddWithValue("Commission", GetDecimalFromString(orderDetail, "commission"));
        cmd.Parameters.AddWithValue("TakerCommissionRate", GetDecimalFromString(orderDetail, "takerCommissionRate"));
        cmd.Parameters.AddWithValue("TakerCommission", GetDecimalFromString(orderDetail, "takerCommission"));
        cmd.Parameters.AddWithValue("TakerAmount", GetDecimalFromString(orderDetail, "takerAmount"));
        cmd.Parameters.AddWithValue("AdditionalKycVerify", orderDetail.GetProperty("additionalKycVerify").GetInt32());

        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SavePayMethodsAsync(JsonElement orderDetail, NpgsqlConnection conn)
    {
        string? orderNumber = orderDetail.GetProperty("orderNumber").GetString();
        if (orderNumber == null) return;

        await using (var deleteCmd = new NpgsqlCommand(@"DELETE FROM ""public"".""PayMethods"" WHERE ""OrderNumber"" = @OrderNumber", conn))
        {
            deleteCmd.Parameters.AddWithValue("OrderNumber", orderNumber);
            await deleteCmd.ExecuteNonQueryAsync();
        }

        foreach (var payMethod in orderDetail.GetProperty("payMethods").EnumerateArray())
        {
            long payMethodId = payMethod.GetProperty("id").GetInt64();

            await using (var pmCmd = new NpgsqlCommand(@"
                INSERT INTO ""public"".""PayMethods"" (""PayMethodId"", ""OrderNumber"", ""Identifier"", ""TradeMethodName"", ""IconUrlColor"")
                VALUES (@PayMethodId, @OrderNumber, @Identifier, @TradeMethodName, @IconUrlColor)
                ON CONFLICT (""PayMethodId"", ""OrderNumber"") DO NOTHING;", conn))
            {
                pmCmd.Parameters.AddWithValue("PayMethodId", payMethodId);
                pmCmd.Parameters.AddWithValue("OrderNumber", orderNumber);
                pmCmd.Parameters.AddWithValue("Identifier", payMethod.GetProperty("identifier").GetString());
                pmCmd.Parameters.AddWithValue("TradeMethodName", payMethod.GetProperty("tradeMethodName").GetString());
                pmCmd.Parameters.AddWithValue("IconUrlColor", payMethod.GetProperty("iconUrlColor").GetString());
                await pmCmd.ExecuteNonQueryAsync();
            }

            var _fields = orderDetail.TryGetProperty("fields", out var fields) ? fields.GetInt64() : 0;
            if (_fields > 0)
            {
                foreach (var field in payMethod.GetProperty("fields").EnumerateArray())
                {
                    await using (var fCmd = new NpgsqlCommand(@"
                    INSERT INTO ""public"".""PayMethodFields"" (
                        ""FieldId"", ""PayMethodId"", ""OrderNumber"", ""FieldName"", ""FieldContentType"", ""RestrictionType"", ""LengthLimit"",
                        ""IsRequired"", ""IsCopyable"", ""HintWord"", ""FieldValue""
                    ) VALUES (
                        @FieldId, @PayMethodId, @OrderNumber, @FieldName, @FieldContentType, @RestrictionType, @LengthLimit,
                        @IsRequired, @IsCopyable, @HintWord, @FieldValue
                    )
                    ON CONFLICT (""FieldId"", ""PayMethodId"", ""OrderNumber"") DO NOTHING;", conn))
                    {
                        fCmd.Parameters.AddWithValue("FieldId", field.GetProperty("fieldId").GetString());
                        fCmd.Parameters.AddWithValue("PayMethodId", payMethodId);
                        fCmd.Parameters.AddWithValue("OrderNumber", orderNumber);
                        fCmd.Parameters.AddWithValue("FieldName", field.GetProperty("fieldName").GetString());
                        fCmd.Parameters.AddWithValue("FieldContentType", field.GetProperty("fieldContentType").GetString());
                        fCmd.Parameters.AddWithValue("RestrictionType", field.GetProperty("restrictionType").GetInt32());
                        fCmd.Parameters.AddWithValue("LengthLimit", field.GetProperty("lengthLimit").GetInt32());
                        fCmd.Parameters.AddWithValue("IsRequired", field.GetProperty("isRequired").GetBoolean());
                        fCmd.Parameters.AddWithValue("IsCopyable", field.GetProperty("isCopyable").GetBoolean());
                        if (field.TryGetProperty("hintWord", out JsonElement hintWordElement))
                        {
                            fCmd.Parameters.AddWithValue("HintWord", hintWordElement.GetString());
                        }
                        else
                        {
                            fCmd.Parameters.AddWithValue("HintWord", DBNull.Value); // o cualquier valor por defecto
                        }

                        fCmd.Parameters.AddWithValue("FieldValue", field.GetProperty("fieldValue").GetString());
                        await fCmd.ExecuteNonQueryAsync();
                    }
                }
            }
        }
    }

    private static decimal GetDecimalFromString(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property)) return 0;
        if (property.ValueKind == JsonValueKind.String && decimal.TryParse(property.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out decimal result)) return result;
        if (property.ValueKind == JsonValueKind.Number) return property.GetDecimal();
        return 0;
    }

    private static string GenerateSignature(string dataToSign, string apiSecret)
    {
        byte[] keyBytes = Encoding.UTF8.GetBytes(apiSecret);
        byte[] dataBytes = Encoding.UTF8.GetBytes(dataToSign);

        using (var hmac = new HMACSHA256(keyBytes))
        {
            byte[] hash = hmac.ComputeHash(dataBytes);
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }
    }
}