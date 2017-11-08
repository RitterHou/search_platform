# coding=utf-8
from elasticsearch import Elasticsearch

__author__ = 'liuzm'

es = Elasticsearch('http://192.168.65.167:9200/')

d2c_index_name = 'd2c-trade-test'
body = {
    "logistics": {"logisticCode": None, "shipTypeName": "快递", "shipTypeCode": "express", "standardCorpCode": None,
                  "corpName": None, "corpCode": None},
    "inOms": False,
    "tradeAble": {"deliverable": False, "priceModifiable": True, "payStatusModifiable": False, "passAuditable": False,
                  "returnAppliable": False, "remarkModifiable": True, "auditCancelable": False,
                  "logisticsModifiable": True, "cancelable": True, "refundable": False, "returnable": False,
                  "payConfirmable": False, "returnReceivable": False, "completable": False, "notPassAuditable": False,
                  "invoicable": False, "refundAppliable": False},
    "consignee": {"selfPickupSite": None, "selfPickupAddress": None, "receiverName": "斑布斑布",
                  "receiverAddress": "软件大道118号新华汇a1栋千米网", "receiverProvince": "江苏省", "receiverCity": "南京市",
                  "fullConsigneeAddress": "江苏省南京市雨花台区软件大道118号新华汇a1栋千米网", "receiverMobile": "15156788765",
                  "receiverDistrict": "雨花台区", "selfPickupPhone": None},
    "sellerMark": None,
    "invoice": {"invoiceContent": None, "invoiceName": "江苏千米网络科技股份有限公司", "invoiceAddress": None,
                "invoiceTaxerCode": None, "invoiceBank": None, "invoiceType": 1, "invoicePhone": None,
                "invoiceAccount": None},
    "payStatus": "UNPAID",
    "salesman": {"salesmanId": None, "salesmanName": None},
    "shipAddress": None,
    "invoiced": False,
    "completeStatus": "DOING",
    "autoConfirmTime": None,
    "autoConfirmSeconds": None,
    "sellerTotalStatus": "WAIT_PAY",
    "seller": {"sellCode": "A1715002", "sellName": None},
    "tradeUrl": None,
    "deviceType": "SELLER_PC",
    "modifyTime": "2017-09-20 16:03:17",
    "employee": {"employeeCode": "A1715002", "employeeName": "卖家"},
    "returnAllowTime": None,
    "buyerMark": "请发申通快递！！",
    "shipTime": None,
    "buyerTotalStatus": "WAIT_PAY",
    "returnStatus": "NOT_APPLIED",
    "finance": {"shipPrice": 0, "totalPrice": 1499, "totalTradePrivilegePrice": 0, "totalMarketingScore": 0,
                "offsetScoreRate": 100, "totalPaidPrice": 0, "actualRefundPrice": None, "totalCouponPrice": 0,
                "totalCutDownPrice": 0, "settleRefundPrice": None, "totalCostPrice": 1000, "refundOffsetScorePrice": 0,
                "totalItemPrice": 1499, "totalPayPrice": 1543.97, "invoicePrice": 44.97, "totalOffsetScoreNum": 0,
                "returnOffsetScorePrice": None, "totalRefundPrice": 0, "tid": "TC17092016031726725049",
                "totalBuyPrice": 1499, "totalRetailPrice": 1499, "returnOffsetScore": None, "totalOffsetScorePrice": 0,
                "totalPresentScore": 0, "refundOffsetScoreNum": 0, "refundSettlePrice": None, "acctRefundPrice": 0},
    "returnAddress": None,
    "itemOrders": [
        {"presentScore": 0, "initItemNum": 1, "spuId": "3006416", "payPrice": 1499, "cutDownPrice": 0, "unit": "件",
         "totalItemPrivilegePrice": 0, "skuPic": "1715002/1f69c7075f92ccd5bcce7165db961d76.jpg", "specification": "",
         "retailPrice": 1499, "gift": False, "totalCostPrice": 1000, "costPrice": 1000, "itemNum": 1,
         "spuName": "哈曼卡顿Harman Kardon Aura Studio2", "packagesId": "78be15a1da004d98af0fe9cd3c30dce3",
         "specificationDetail": {}, "skuId": "g6510943", "beforePayItemNum": 1, "brand": "哈曼卡顿",
         "oid": "O17092016031726763053", "skuName": "哈曼卡顿Harman Kardon Aura Studio2 ", "tid": "TC17092016031726725049",
         "totalBuyPrice": 1499, "totalRetailPrice": 1499, "packages": False, "createTime": "2017-09-20 16:03:17",
         "totalCutDownPrice": 0, "totalPresentScore": 0, "showed": False, "buyPrice": 1499, "skuBn": "4490360521561246",
         "barCode": None, "returnedItemNum": 0, "totalPayPrice": 1499, "overSell": False, "commented": False}],
    "payment": {"typeId": "OBP", "bankName": None, "no": None, "typeName": "余额支付", "gateName": None, "bankCode": None,
                "gateCode": None, "payTime": None},
    "fromIp": None,
    "needInvoice": True,
    "tid": "TC17092016031726725049",
    "shipStatus": "UNSHIPPED",
    "tradeAbleBuyer": {"applyRefundCancelable": False, "confirmable": False, "applying": False,
                       "refundAppliable": False, "returnAppliable": False, "creatable": False, "commentable": False,
                       "cancelable": True, "payable": True, "applyReturnCancelable": False, "logisticsViewable": False},
    "buyer": {"idCard": None, "buyUserCode": "C00000001383945", "buyNickName": "m15150657027"},
    "autoCloseTime": "2017-09-23 16:03:17",
    "createTime": "2017-09-20 16:03:17",
    "settleStatus": None,
    "shipReceipt": None,
    "refundStatus": "NOT_APPLIED",
    "_adminId": "A1715002",
    "applies": None,
    "endTime": None,
    "commented": False
}

# es.index(d2c_index_name, 'Trade', id='TC17092016031726725049', body=body)

update_body = {"tid": "TC17092016031726725049", "createTime": "2017-09-20 16:03:18",
               "modifyTime": "2017-09-20 16:03:17", "endTime": None, "autoCloseTime": "2017-09-23 16:03:18",
               "shipTime": None, "autoConfirmTime": None, "autoConfirmSeconds": None, "returnAllowTime": None,
               "completeStatus": "DOING", "payStatus": "PAID", "settleStatus": None, "shipStatus": "UNSHIPPED",
               "commented": False, "returnStatus": "NOT_APPLIED", "refundStatus": "NOT_APPLIED",
               "sellerTotalStatus": "WAIT_SHIP", "buyerTotalStatus": "WAIT_SHIP",
               "buyer": {"buyUserCode": "C00000001383945", "buyNickName": "m15150657027", "idCard": None},
               "seller": {"sellCode": "A1715002", "sellName": None},
               "employee": {"employeeCode": "A1715002", "employeeName": "卖家"}, "salesman": None,
               "payment": {"typeId": "OBP", "typeName": "余额支付", "gateCode": None, "gateName": None, "bankCode": None,
                           "bankName": None, "no": "M201709200084034039", "payTime": "2017-09-20 16:03:17"},
               "needInvoice": True, "invoiced": False,
               "invoice": {"invoiceType": 1, "invoiceName": "江苏千米网络科技股份有限公司", "invoiceContent": None,
                           "invoiceTaxerCode": None, "invoiceAddress": None, "invoicePhone": None, "invoiceBank": None,
                           "invoiceAccount": None},
               "logistics": {"shipTypeCode": "express", "shipTypeName": "快递", "logisticCode": None, "corpCode": None,
                             "corpName": None, "standardCorpCode": None}, "fromIp": None, "deviceType": "SELLER_PC",
               "buyerMark": "请发申通快递！！", "sellerMark": None, "tradeUrl": None,
               "finance": {"tid": "TC17092016031726725049", "shipPrice": 0.000, "totalCouponPrice": 0.000,
                           "totalTradePrivilegePrice": 0.000000, "totalCutDownPrice": 0.000, "totalBuyPrice": 1499.00,
                           "totalItemPrice": 1499.0000, "totalPrice": 1499.0000, "totalPayPrice": 1543.9700,
                           "totalPaidPrice": 1543.9700, "totalRetailPrice": 1499.0000, "totalCostPrice": 1000.0000,
                           "totalOffsetScorePrice": 0.0000, "offsetScoreRate": 100, "totalOffsetScoreNum": 0.0000,
                           "totalPresentScore": 0.0000, "totalMarketingScore": 0.0000, "refundOffsetScorePrice": 0.0000,
                           "refundOffsetScoreNum": 0.0000, "totalRefundPrice": 0.0000, "acctRefundPrice": 0.0000,
                           "settleRefundPrice": None, "invoicePrice": 44.970, "refundSettlePrice": None,
                           "actualRefundPrice": None, "returnOffsetScorePrice": None, "returnOffsetScore": None},
               "itemOrders": [{"oid": "O17092016031726763053", "tid": "TC17092016031726725049",
                               "createTime": "2017-09-20 16:03:18", "spuId": "3006416",
                               "spuName": "哈曼卡顿Harman Kardon Aura Studio2", "skuId": "g6510943",
                               "skuName": "哈曼卡顿Harman Kardon Aura Studio2 ",
                               "skuPic": "1715002/1f69c7075f92ccd5bcce7165db961d76.jpg", "skuBn": "4490360521561246",
                               "specification": "", "specificationDetail": {}, "brand": "哈曼卡顿", "barCode": None,
                               "unit": "件", "packagesId": "78be15a1da004d98af0fe9cd3c30dce3", "commented": False,
                               "showed": False, "overSell": False, "initItemNum": 1.00, "beforePayItemNum": 1.00,
                               "itemNum": 1.00, "returnedItemNum": 0.00, "buyPrice": 1499.00, "totalBuyPrice": 1499.00,
                               "totalItemPrivilegePrice": 0.00, "payPrice": 1499.00, "totalPayPrice": 1499.00,
                               "costPrice": 1000.00, "totalCostPrice": 1000.00, "retailPrice": 1499.00,
                               "totalRetailPrice": 1499.00, "cutDownPrice": 0.0000, "totalCutDownPrice": 0.0000,
                               "presentScore": 0.00, "totalPresentScore": 0.00, "packages": False, "gift": False}],
               "applies": None, "shipReceipt": None,
               "consignee": {"receiverName": "斑布斑布", "receiverProvince": "江苏省", "receiverCity": "南京市",
                             "receiverDistrict": "雨花台区", "receiverAddress": "软件大道118号新华汇a1栋千米网",
                             "receiverMobile": "15156788765", "selfPickupAddress": None, "selfPickupSite": None,
                             "selfPickupPhone": None, "fullConsigneeAddress": "江苏省南京市雨花台区软件大道118号新华汇a1栋千米网"},
               "tradeAble": {"cancelable": False, "priceModifiable": False, "remarkModifiable": True,
                             "payStatusModifiable": False, "completable": False, "payConfirmable": False,
                             "deliverable": False, "returnable": False, "refundable": False, "invoicable": False,
                             "logisticsModifiable": False, "returnAppliable": False, "refundAppliable": True,
                             "passAuditable": False, "notPassAuditable": False, "auditCancelable": False,
                             "returnReceivable": False},
               "tradeAbleBuyer": {"creatable": False, "cancelable": False, "payable": False, "confirmable": False,
                                  "commentable": False, "refundAppliable": True, "returnAppliable": False,
                                  "applyRefundCancelable": False, "applyReturnCancelable": False, "applying": False,
                                  "logisticsViewable": False}, "shipAddress": None, "returnAddress": None,
               "inOms": True}

es.index(d2c_index_name, 'Trade', id='TC17092016031726725049', body=update_body)