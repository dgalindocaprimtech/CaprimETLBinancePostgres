using Microsoft.Extensions.Configuration;
using Npgsql;
using System.Data;
using ExcelDataReader;

/// <summary>
/// Clase dedicada a procesar las actualizaciones de KYC desde un archivo Excel.
/// </summary>
public class KycUpdateProcessor
{
    /// <summary>
    /// Método principal que orquesta la lectura del archivo y la actualización en la base de datos.
    /// </summary>
    /// <param name="config">La configuración de la aplicación (para obtener la ruta del archivo).</param>
    /// <param name="connectionString">La cadena de conexión a la base de datos PostgreSQL.</param>
    public async Task ProcessAsync(IConfiguration config, string connectionString)
    {
        Console.WriteLine("\n=============================================");
        Console.WriteLine("  Iniciando proceso de actualización KYC...  ");
        Console.WriteLine("=============================================");

        string? filePath = config.GetValue<string>("KycFilePath");
        if (string.IsNullOrEmpty(filePath) || !File.Exists(filePath))
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: La ruta del archivo KYC no es válida o el archivo no existe.");
            Console.WriteLine($"Ruta configurada en appsettings.json: {filePath}");
            Console.ResetColor();
            return;
        }

        Console.WriteLine($"Leyendo archivo de KYC desde: {filePath}");

        try
        {
            using var stream = File.Open(filePath, FileMode.Open, FileAccess.Read);
            using var reader = ExcelReaderFactory.CreateReader(stream);

            var dataSet = reader.AsDataSet(new ExcelDataSetConfiguration
            {
                ConfigureDataTable = (_) => new ExcelDataTableConfiguration { UseHeaderRow = true }
            });

            if (!dataSet.Tables.Contains("Respuestas de formulario 1"))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error: No se encontró la hoja 'Respuestas de formulario 1' en el archivo Excel.");
                Console.ResetColor();
                return;
            }

            var table = dataSet.Tables["Respuestas de formulario 1"];
            if (table == null) return;

            Console.WriteLine($"Se encontraron {table.Rows.Count} registros en el archivo Excel. Procesando...");
            int updatedCount = 0;

            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();

            foreach (DataRow row in table.Rows)
            {
                string orderNumber = row["OrdenID (NO EDITAR)"].ToString() ?? "";
                string operacion = row["OPERACION"].ToString() ?? "";
                string nacionalidad = row["Nacionalidad"].ToString() ?? "";
                if (string.IsNullOrWhiteSpace(orderNumber) || operacion.ToUpper().Equals("PENDIENTE") || string.IsNullOrWhiteSpace(nacionalidad))
                {
                    continue; // Omitir si no hay OrderNumber o la operación está pendiente
                }

                string? takerUserNo = null;
                int? orderStatus = null;

                // 1. Buscar el TakerUserNo y OrderStatus en la tabla OrderDetails
                await using (var cmdCheck = new NpgsqlCommand(@"SELECT ""TakerUserNo"", ""OrderStatus"" FROM ""public"".""OrderDetails"" WHERE ""OrderNumber"" = @OrderNumber", conn))
                {
                    cmdCheck.Parameters.AddWithValue("OrderNumber", orderNumber);
                    await using var dbReader = await cmdCheck.ExecuteReaderAsync();
                    if (await dbReader.ReadAsync())
                    {
                        takerUserNo = dbReader.GetString(0);
                        orderStatus = dbReader.GetInt32(1);
                    }
                }

                // 2. Validar condición: OrderStatus debe ser 4
                if (orderStatus != 4)
                {
                    continue; // Si el estado no es 4, pasar al siguiente registro del Excel
                }

                if (!string.IsNullOrEmpty(takerUserNo))
                {
                    // 3. Realizar la actualización en la tabla Users
                    await using (var cmdUpdate = new NpgsqlCommand(@"
                        UPDATE ""public"".""Users"" SET
                            ""City"" = @City,
                            ""Phone"" = @Phone,
                            ""Email"" = @Email,
                            ""Nationality"" = @Nationality,
                            ""IdentificationFullName"" = @IdentificationFullName,
                            ""IdentificationID"" = @IdentificationID,
                            ""IdentificationType"" = @IdentificationType,
                            ""KycDate"" = @KycDate,
                            ""KycAvailable"" = TRUE
                        WHERE ""TakerUserNo"" = @TakerUserNo;
                    ", conn))
                    {
                        cmdUpdate.Parameters.AddWithValue("TakerUserNo", takerUserNo);
                        cmdUpdate.Parameters.AddWithValue("City", row["Ciudad de residencia"].ToString());
                        cmdUpdate.Parameters.AddWithValue("Phone", row["Número Celular"].ToString());
                        cmdUpdate.Parameters.AddWithValue("Email", row["Email"].ToString());
                        cmdUpdate.Parameters.AddWithValue("Nationality", row["Nacionalidad"].ToString());
                        cmdUpdate.Parameters.AddWithValue("IdentificationFullName", row["NOMBRE"].ToString());
                        cmdUpdate.Parameters.AddWithValue("IdentificationID", row["NUMERO DOCUMENTO"].ToString());
                        cmdUpdate.Parameters.AddWithValue("IdentificationType", row["Tipo documento"].ToString());

                        // Convertir la fecha. Si falla, se inserta DBNull.Value
                        if (DateTime.TryParse(row["Marca temporal"].ToString(), out DateTime kycDate))
                        {
                            cmdUpdate.Parameters.AddWithValue("KycDate", kycDate.Date);
                        }
                        else
                        {
                            cmdUpdate.Parameters.AddWithValue("KycDate", (object)DBNull.Value);
                        }

                        int rowsAffected = await cmdUpdate.ExecuteNonQueryAsync();
                        if (rowsAffected > 0)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"  -> Usuario {takerUserNo} actualizado correctamente para la orden {orderNumber}.");
                            Console.ResetColor();
                            updatedCount++;
                        }
                    }
                }
            }
            Console.WriteLine($"\nProceso de actualización KYC finalizado. Se actualizaron {updatedCount} usuarios.");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"\nOcurrió un error inesperado durante el proceso de KYC: {ex.Message}");
            Console.ResetColor();
        }
    }
}