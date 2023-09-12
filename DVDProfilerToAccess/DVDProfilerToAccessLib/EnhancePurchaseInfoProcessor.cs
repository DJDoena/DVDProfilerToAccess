using System.Collections.Generic;
using System.Text;
using DoenaSoft.ToolBox.Generics;
using EPI = DoenaSoft.DVDProfiler.EnhancedPurchaseInfo;
using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal static class EnhancePurchaseInfoProcessor
    {
        internal static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                var epi = Serializer<EPI.EnhancedPurchaseInfo>.FromString(pluginData.Any[0].OuterXml);

                AddInsertCommand(commands, profile, epi);
            }
        }

        private static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, EPI.EnhancedPurchaseInfo purchaseInfo)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tEnhancedPurchaseInfo VALUES(");
            commandText.Append(SqlProcessor.PrepareTextForDb(profile.ID));
            commandText.Append(", ");

            GetPrice(commandText, purchaseInfo.OriginalPrice);

            commandText.Append(", ");

            GetPrice(commandText, purchaseInfo.ShippingCost);

            commandText.Append(", ");

            GetPrice(commandText, purchaseInfo.CreditCardCharge);

            commandText.Append(", ");

            GetPrice(commandText, purchaseInfo.CreditCardFees);

            commandText.Append(", ");

            GetPrice(commandText, purchaseInfo.Discount);

            commandText.Append(", ");

            GetPrice(commandText, purchaseInfo.CustomsFees);

            commandText.Append(", ");

            GetText(commandText, purchaseInfo.CouponType);

            commandText.Append(", ");

            GetText(commandText, purchaseInfo.CouponCode);

            commandText.Append(", ");

            GetPrice(commandText, purchaseInfo.AdditionalPrice1);

            commandText.Append(", ");

            GetPrice(commandText, purchaseInfo.AdditionalPrice2);

            commandText.Append(", ");

            GetDate(commandText, purchaseInfo.OrderDate);

            commandText.Append(", ");

            GetDate(commandText, purchaseInfo.ShippingDate);

            commandText.Append(", ");

            GetDate(commandText, purchaseInfo.DeliveryDate);

            commandText.Append(", ");

            GetDate(commandText, purchaseInfo.AdditionalDate1);

            commandText.Append(", ");

            GetDate(commandText, purchaseInfo.AdditionalDate2);

            commandText.Append(")");

            commands.Add(commandText);
        }

        private static void GetDate(StringBuilder commandText, EPI.Date date)
        {
            if (date != null)
            {
                SqlProcessor.PrepareDateForDb(commandText, date.Value, false);
            }
            else
            {
                commandText.Append(SqlProcessor.NULL);
            }
        }

        private static void GetText(StringBuilder commandText, EPI.Text text)
        {
            if (text != null)
            {
                commandText.Append(SqlProcessor.PrepareOptionalTextForDb(text.Value));
            }
            else
            {
                commandText.Append(SqlProcessor.NULL);
            }
        }

        private static void GetPrice(StringBuilder commandText, EPI.Price price)
        {
            if (price != null)
            {
                commandText.Append(SqlProcessor.PrepareOptionalTextForDb(price.DenominationType));
                commandText.Append(", ");
                commandText.Append(price.Value.ToString(SqlProcessor.FormatInfo));
            }
            else
            {
                commandText.Append(SqlProcessor.NULL);
                commandText.Append(", ");
                commandText.Append(SqlProcessor.NULL);
            }
        }
    }
}