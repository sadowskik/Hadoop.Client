using System;
using System.IO;
using System.Text;

namespace Hadoop.Client
{    
    internal static class StringExtensions
    {
        public static Stream ToUtf8Stream(this string value)
        {
            var result = new MemoryStream();
            using (var temp = new MemoryStream())
            using (var writer = new StreamWriter(temp, Encoding.UTF8))
            {
                writer.Write(value);
                writer.Flush();
                temp.Flush();
                temp.Position = 0;
                temp.CopyTo(result);
            }
            result.Position = 0;
            return result;
        }

        public static string EscapeDataString(this string inputValue)
        {
            return inputValue.IsNullOrEmpty()
                ? string.Empty
                : Uri.EscapeDataString(inputValue);
        }

        public static bool IsNullOrEmpty(this string value)
        {
            return string.IsNullOrEmpty(value);
        }

        public static bool IsNotNullOrEmpty(this string value)
        {
            return !string.IsNullOrEmpty(value);
        }
    }
}
