using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using EPI = DoenaSoft.DVDProfiler.EnhancedPurchaseInfo;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal static class EnhancePurchaseInfoProcessor
    {
        internal static void GetInsertCommand(List<String> sqlCommands
            , DVD dvd
            , PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                using (StringReader sr = new StringReader(pluginData.Any[0].OuterXml))
                {
                    EPI.EnhancedPurchaseInfo epi = (EPI.EnhancedPurchaseInfo)(EPI.EnhancedPurchaseInfo.XmlSerializer.Deserialize(sr));

                    GetInsertCommand(sqlCommands, dvd, epi);
                }
            }
        }

        private static void GetInsertCommand(List<String> sqlCommands
            , DVD dvd
            , EPI.EnhancedPurchaseInfo epi)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tEnhancedPurchaseInfo VALUES(");
            insertCommand.Append(SqlProcessor.PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            GetPrice(insertCommand, epi.OriginalPrice);

            insertCommand.Append(", ");

            GetPrice(insertCommand, epi.ShippingCost);

            insertCommand.Append(", ");

            GetPrice(insertCommand, epi.CreditCardCharge);

            insertCommand.Append(", ");

            GetPrice(insertCommand, epi.CreditCardFees);

            insertCommand.Append(", ");

            GetPrice(insertCommand, epi.Discount);

            insertCommand.Append(", ");

            GetPrice(insertCommand, epi.CustomsFees);

            insertCommand.Append(", ");

            GetText(insertCommand, epi.CouponType);

            insertCommand.Append(", ");

            GetText(insertCommand, epi.CouponCode);

            insertCommand.Append(", ");

            GetPrice(insertCommand, epi.AdditionalPrice1);

            insertCommand.Append(", ");

            GetPrice(insertCommand, epi.AdditionalPrice2);

            insertCommand.Append(", ");

            GetDate(insertCommand, epi.OrderDate);

            insertCommand.Append(", ");

            GetDate(insertCommand, epi.ShippingDate);

            insertCommand.Append(", ");

            GetDate(insertCommand, epi.DeliveryDate);

            insertCommand.Append(", ");

            GetDate(insertCommand, epi.AdditionalDate1);

            insertCommand.Append(", ");

            GetDate(insertCommand, epi.AdditionalDate2);

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private static void GetDate(StringBuilder insertCommand
            , EPI.Date date)
        {
            if (date != null)
            {
                SqlProcessor.PrepareDateForDb(insertCommand, date.Value, false);
            }
            else
            {
                insertCommand.Append(SqlProcessor.NULL);
            }
        }

        private static void GetText(StringBuilder insertCommand
            , EPI.Text text)
        {
            if (text != null)
            {
                insertCommand.Append(SqlProcessor.PrepareOptionalTextForDb(text.Value));
            }
            else
            {
                insertCommand.Append(SqlProcessor.NULL);
            }
        }

        private static void GetPrice(StringBuilder insertCommand
            , EPI.Price price)
        {
            if (price != null)
            {
                insertCommand.Append(SqlProcessor.PrepareOptionalTextForDb(price.DenominationType));
                insertCommand.Append(", ");
                insertCommand.Append(price.Value.ToString(CultureInfo.GetCultureInfo("en-US")));
            }
            else
            {
                insertCommand.Append(SqlProcessor.NULL);
                insertCommand.Append(", ");
                insertCommand.Append(SqlProcessor.NULL);
            }
        }
    }
}