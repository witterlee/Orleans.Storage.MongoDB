using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Orleans.Storage.MongoDB
{
    public class UnixDateTimeConverter : DateTimeConverterBase
    {
        private DateTime initialTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value is DateTime)
            {
                DateTime currentValue = ((DateTime)value).ToUniversalTime();
                TimeSpan span = currentValue - initialTime;
                writer.WriteValue(span.TotalSeconds);
            }
            else
                throw new ArgumentException("Must provide a valid datetime struct", "value");

        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            try
            {
                long ticks = Convert.ToInt64(reader.Value);
                return initialTime.AddSeconds(ticks);
            }
            catch { }

            try
            {
                return Convert.ToDateTime(reader.Value.ToString());
            }
            catch { }

            if (objectType == typeof(DateTime?)) return null;
            else
                throw new Exception("can not read value from json");
        }
    }
}
