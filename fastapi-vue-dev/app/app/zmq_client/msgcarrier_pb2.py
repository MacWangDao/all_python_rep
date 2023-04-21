# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: msgcarrier.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


# import typedef_pb2 as typedef__pb2
from app.zmq_client import typedef_pb2 as typedef__pb2
from app.zmq_client.typedef_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10msgcarrier.proto\x12\rPB.MSGCARRIER\x1a\rtypedef.proto\"\x85@\n\nMsgCarrier\x12\r\n\x05seqno\x18\x01 \x01(\x03\x12/\n\x04type\x18\x02 \x01(\x0e\x32!.PB.MSGCARRIER.MsgCarrier.MsgType\x12\x0f\n\x07message\x18\x03 \x01(\x0c\x12\x11\n\tindicator\x18\x04 \x01(\x05\x12\x0b\n\x03jwt\x18\x05 \x01(\t\x12\x15\n\rauthenticated\x18\x06 \x01(\x08\x12\x0f\n\x07service\x18\x07 \x01(\t\x12\x10\n\x08pubtopic\x18\x08 \x01(\t\x12\r\n\x05msgid\x18\t \x01(\x03\x12\x12\n\niscompress\x18\n \x01(\x08\x12!\n\x19message_routing_identifer\x18\x0b \x01(\t\"\x85>\n\x07MsgType\x12\x18\n\x0bMSG_UNKNOWN\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01\x12\x0f\n\x0bINSERTORDER\x10\x00\x12\x12\n\x0eINSERTORDERREP\x10\x01\x12\x0f\n\x0b\x43\x41NCELORDER\x10\x02\x12\x12\n\x0e\x43\x41NCELORDERREP\x10\x03\x12\x0e\n\nQUERYORDER\x10\x04\x12\x11\n\rQUERYORDERREP\x10\x05\x12\x11\n\rQUERYPOSITION\x10\x06\x12\x14\n\x10QUERYPOSITIONREP\x10\x07\x12\x10\n\x0cQUERYACCOUNT\x10\x08\x12\x13\n\x0fQUERYACCOUNTREP\x10\t\x12\x0e\n\nQUERYTRADE\x10\n\x12\x11\n\rQUERYTRADEREP\x10\x0b\x12\r\n\tERRORTYPE\x10\x0c\x12\r\n\tINSERTJOB\x10\r\x12\x10\n\x0cINSERTJOBREP\x10\x0e\x12\x10\n\x0cJOBRISKCHECK\x10\x0f\x12\x13\n\x0fJOBRISKCHECKREP\x10\x10\x12\x0c\n\x08PAUSEJOB\x10\x11\x12\x0f\n\x0bPAUSEJOBREP\x10\x12\x12\x0e\n\nRESTARTJOB\x10\x13\x12\x11\n\rRESTARTJOBREP\x10\x14\x12\x0b\n\x07STOPJOB\x10\x15\x12\x0e\n\nSTOPJOBREP\x10\x16\x12\r\n\tRISKCHECK\x10\x17\x12\x10\n\x0cRISKCHECKREP\x10\x18\x12\x15\n\x11SECURITYCODETABLE\x10\x19\x12\x0c\n\x08SNAPSHOT\x10\x1a\x12\x0f\n\x0bTRANSACTION\x10\x1b\x12\x12\n\x0eQUERYCODETABLE\x10\x1c\x12\x14\n\x10QUERYSNAPSHOTALL\x10\x1d\x12\x0e\n\nJOBCOMMAND\x10\x1e\x12\t\n\x05LOGIN\x10\x1f\x12\x0c\n\x08LOGINREP\x10 \x12\x0c\n\x08USERAUTH\x10!\x12\x0f\n\x0bUSERAUTHREP\x10\"\x12\r\n\tBASICDATA\x10#\x12\x11\n\rPUB_JOBUPDATE\x10$\x12\x13\n\x0fPUB_ORDERUPDATE\x10%\x12\x14\n\x10\x41LGO_QUERYJOBALL\x10&\x12\x12\n\x0e\x41LGO_QUERYJOBS\x10\'\x12\x19\n\x15\x41LGO_QUERYJOB4ACCOUNT\x10(\x12\x17\n\x13\x41LGO_QUERYJOBALLREP\x10)\x12\x16\n\x12\x41LGO_QUERYORDERALL\x10*\x12\x14\n\x10\x41LGO_QUERYORDERS\x10+\x12\x1b\n\x17\x41LGO_QUERYORDER4ACCOUNT\x10,\x12\x17\n\x13\x41LGO_QUERYORDER4JOB\x10-\x12\x19\n\x15\x41LGO_QUERYORDERALLREP\x10.\x12\x1c\n\x18\x42\x41SICDATA_STOCKALPHABETA\x10/\x12\x19\n\x15\x42\x41SICDATA_INDEXSTATUS\x10\x30\x12\x1f\n\x1b\x42\x41SICDATA_SECURITYCODETABLE\x10\x31\x12\x1d\n\x19\x42\x41SICDATA_SECTORCODETABLE\x10\x32\x12\x1c\n\x18\x42\x41SICDATA_FUTUREFORECAST\x10\x33\x12\x1b\n\x17\x42\x41SICDATA_INDEXFORECAST\x10\x34\x12\x1b\n\x17\x42\x41SICDATA_STOCKBASEINFO\x10\x35\x12\x1f\n\x1b\x42\x41SICDATA_INDEXCONSTITUENTS\x10\x36\x12\x1f\n\x1b\x42\x41SICDATA_INDUSCONSTITUENTS\x10\x37\x12\x1a\n\x16\x42\x41SICDATA_FUNDINDEXMAP\x10\x38\x12 \n\x1c\x42\x41SICDATA_INDEXPRICEFORECAST\x10\x39\x12 \n\x1c\x42\x41SICDATA_STOCKPRICEFORECAST\x10:\x12\x1c\n\x18\x42\x41SICDATA_STOCKRISKVALUE\x10;\x12\x1a\n\x16\x42\x41SICDATA_SECTORWEIGHT\x10<\x12\x1b\n\x17\x42\x41SICDATA_FUNDALPHABETA\x10=\x12\x1c\n\x18\x41LGO_QUERYJOB4ACCOUNTREP\x10>\x12\x17\n\x13\x41LGO_QUERYORDERSREP\x10?\x12\x1e\n\x1a\x41LGO_QUERYORDER4ACCOUNTREP\x10@\x12\x1a\n\x16\x41LGO_QUERYORDER4JOBREP\x10\x41\x12\x0f\n\x0bUSERHEARTBT\x10\x42\x12\x14\n\x10SNAPSHOTCOMPRESS\x10\x43\x12\x12\n\x0ePUBACCOUNTINFO\x10\x46\x12\x10\n\x0cPUBPOSITIONS\x10I\x12\x18\n\x14QUERYPOSITIONSUMMARY\x10J\x12\x1b\n\x17QUERYPOSITIONSUMMARYREP\x10K\x12\x16\n\x12PUBPOSITIONSUMMARY\x10L\x12\r\n\tPUBTRADES\x10O\x12\r\n\tPUBORDERS\x10R\x12\x11\n\rQUERYTDPROFIT\x10S\x12\x14\n\x10QUERYTDPROFITREP\x10T\x12\x0f\n\x0bPUBTDPROFIT\x10U\x12\x15\n\x11\x41LGO_QUERYJOBSREP\x10V\x12\x11\n\rJOBCOMMANDREP\x10W\x12\x18\n\x14\x43ONFIG_QUERYPRODLIST\x10X\x12\x1b\n\x17\x43ONFIG_QUERYPRODLISTREP\x10Y\x12\x1c\n\x18\x43ONFIG_QUERYACCOUNT4PROD\x10Z\x12\x1f\n\x1b\x43ONFIG_QUERYACCOUNT4PRODREP\x10[\x12\x19\n\x15\x42\x41SICDATA_MARKETSTYLE\x10\\\x12\r\n\tSNAPSHOTS\x10]\x12\x10\n\x0cTRANSACTIONS\x10^\x12\x10\n\x0cRISK_TESTMSG\x10_\x12\x12\n\x0eRISK_OVERNIGHT\x10`\x12\x12\n\x0eQUERYSNAPSHOTS\x10\x61\x12\x14\n\x10QUERYSNAPSHOTREP\x10\x62\x12\x15\n\x11HEDGE_STATEUPDATE\x10\x63\x12\x1c\n\x18\x44\x45RIVEDDATA_LFSIGNAL_REQ\x10\x64\x12\x1c\n\x18\x44\x45RIVEDDATA_LFSIGNAL_REP\x10\x65\x12\x13\n\x0eHEDGE_SETPARAM\x10\x83\x01\x12\x16\n\x11HEDGE_SETPARAMREP\x10\x84\x01\x12\x1c\n\x17HEDGE_SETPARAMEXTENDREP\x10\x85\x01\x12\x13\n\x0eHEDGE_GETPARAM\x10\x86\x01\x12\x16\n\x11HEDGE_GETPARAMREP\x10\x87\x01\x12\x17\n\x12HEDGE_GETREFSIGNAL\x10\x88\x01\x12\x1a\n\x15HEDGE_GETREFSIGNALREP\x10\x89\x01\x12\x17\n\x12HEDGE_CONTRACTCODE\x10\x8a\x01\x12\x1a\n\x15HEDGE_CONTRACTCODEREP\x10\x8b\x01\x12\x17\n\x12HEDGE_GETSFACCOUNT\x10\x8c\x01\x12\x1a\n\x15HEDGE_GETSFACCOUNTREP\x10\x8d\x01\x12\x17\n\x12QUERY_SNAPLITESALL\x10\x8e\x01\x12\x14\n\x0fQUERY_SNAPLITES\x10\x8f\x01\x12\x17\n\x12QUERY_SNAPLITESREP\x10\x90\x01\x12\x17\n\x12PUB_SNAPLITEUPDATE\x10\x91\x01\x12\x11\n\x0cRISK_SETTING\x10\x92\x01\x12\x14\n\x0fRISK_SETTINGREP\x10\x93\x01\x12\x1b\n\x16RISK_RISKSETTINGUPDATE\x10\x94\x01\x12\x1e\n\x19RISK_RISKSETTINGUPDATEREP\x10\x95\x01\x12\x19\n\x14RISK_GETPOSITIONBETA\x10\x96\x01\x12\x1c\n\x17RISK_GETPOSITIONBETAREP\x10\x97\x01\x12\x17\n\x12RISK_GETRISKREASON\x10\x98\x01\x12\x1a\n\x15RISK_GETRISKREASONREP\x10\x99\x01\x12\x18\n\x13RISK_GETRISKSETTING\x10\x9a\x01\x12\x1b\n\x16RISK_GETRISKSETTINGREP\x10\x9b\x01\x12\x1b\n\x16RISK_GETALLRISKSETTING\x10\x9c\x01\x12\x1e\n\x19RISK_GETALLRISKSETTINGREP\x10\x9d\x01\x12\x13\n\x0ePUB_JOBSUMMARY\x10\x9e\x01\x12!\n\x1c\x44\x45RIVEDDATA_ENTRUSTDEPTH_REP\x10\x9f\x01\x12!\n\x1c\x44\x45RIVEDDATA_ENTRUSTDEPTH_REQ\x10\xa0\x01\x12\x14\n\x0fQUERY_THRESHOLD\x10\xa1\x01\x12\x17\n\x12QUERY_THRESHOLDREP\x10\xa2\x01\x12\x14\n\x0fQUOTE_HEARTBEAT\x10\xa3\x01\x12\x14\n\x0fHFTRADING_LOGIN\x10\xa4\x01\x12\x17\n\x12HFTRADING_LOGINREP\x10\xa5\x01\x12\x1b\n\x16HFTRADING_ACCOUNTLOGIN\x10\xa6\x01\x12 \n\x1bHFTRADING_ADDUPDATESTRATEGY\x10\xa7\x01\x12#\n\x1eHFTRADING_ADDUPDATESTRATEGYREP\x10\xa8\x01\x12\x1b\n\x16HFTRADING_STRATEGYSREQ\x10\xab\x01\x12\x1b\n\x16HFTRADING_STRATEGYSREP\x10\xac\x01\x12\x1d\n\x18HFTRADING_DELETESTRATEGY\x10\xad\x01\x12 \n\x1bHFTRADING_DELETESTRATEGYREP\x10\xae\x01\x12\x1c\n\x17HFTRADING_STARTSTRATEGY\x10\xaf\x01\x12 \n\x1bHFTRADING_ACTIONSTRATEGYREP\x10\xb0\x01\x12\x1e\n\x19HFTRADING_SUSPENDSTRATEGY\x10\xb1\x01\x12\x1b\n\x16HFTRADING_STOPSTRATEGY\x10\xb2\x01\x12\x1b\n\x16HFTRADING_CONTRACTSREQ\x10\xb3\x01\x12\x1b\n\x16HFTRADING_CONTRACTSREP\x10\xb4\x01\x12\x1e\n\x19HFTRADING_STRATEGYVIEWPUB\x10\xb5\x01\x12\x1b\n\x16\x42\x41SICDATA_FUNDBASEINFO\x10\xb6\x01\x12!\n\x1c\x42\x41SICDATA_CLIENTSECTORWEIGHT\x10\xb7\x01\x12\x1e\n\x19\x42\x41SICDATA_BASICUPDATETIME\x10\xb8\x01\x12\x1d\n\x18\x43ONFIG_QUERYFEES4ACCOUNT\x10\xb9\x01\x12 \n\x1b\x43ONFIG_QUERYFEES4ACCOUNTREP\x10\xba\x01\x12!\n\x1c\x44\x45RIVEDDATA_MFANDPERCENT_REQ\x10\xbb\x01\x12!\n\x1c\x44\x45RIVEDDATA_MFANDPERCENT_REP\x10\xbc\x01\x12$\n\x1f\x44\x45RIVEDDATA_RANGEPROJECTION_REQ\x10\xbd\x01\x12$\n\x1f\x44\x45RIVEDDATA_RANGEPROJECTION_REP\x10\xbe\x01\x12\x1f\n\x1aHFTRADING_STRATEGYVIEWSREQ\x10\xbf\x01\x12\x1f\n\x1aHFTRADING_STRATEGYVIEWSREP\x10\xc0\x01\x12\x1e\n\x19HFTRADING_HFQUERYTRADEREQ\x10\xc1\x01\x12\x1e\n\x19HFTRADING_HFQUERYTRADEREP\x10\xc2\x01\x12\x1c\n\x17HFTRADING_SPREADDATAPUB\x10\xc3\x01\x12\x1c\n\x17HFTRADING_PROFITDATAPUB\x10\xc4\x01\x12!\n\x1cHFTRADING_QUERYPROFITDATAREQ\x10\xc5\x01\x12!\n\x1cHFTRADING_QUERYPROFITDATAREP\x10\xc6\x01\x12#\n\x1eHFTRADING_STRATEGYTHRESHOLDPUB\x10\xc7\x01\x12!\n\x1cHFTRADING_QUERYINSTRUMENTRSP\x10\xc8\x01\x12\x15\n\x10REDIS_COMMANDREQ\x10\xc9\x01\x12\x15\n\x10REDIS_COMMANDREP\x10\xca\x01\x12%\n RISK_SAVE_COMPANYRISKSETTING_REQ\x10\xcb\x01\x12%\n RISK_SAVE_COMPANYRISKSETTING_REP\x10\xcc\x01\x12$\n\x1fRISK_GET_COMPANYRISKSETTING_REQ\x10\xcd\x01\x12$\n\x1fRISK_GET_COMPANYRISKSETTING_REP\x10\xce\x01\x12\x0e\n\tModifyPwd\x10\xcf\x01\x12\x11\n\x0cModifyPwdRep\x10\xd0\x01\x12\x13\n\x0ePUBALLPOSITION\x10\xd1\x01\x12\x0f\n\nQueryQuote\x10\xd2\x01\x12\x12\n\rQueryQuoteRep\x10\xd3\x01\x12\x10\n\x0bSYSTEMLOGIN\x10\xd4\x01\x12\x13\n\x0eSYSTEMLOGINREP\x10\xd5\x01\x12\x12\n\rSMM_QUERY_REQ\x10\xd6\x01\x12\x12\n\rSMM_QUERY_REP\x10\xd7\x01\x12\x13\n\x0eSMM_MODIFY_REQ\x10\xd8\x01\x12\x13\n\x0eSMM_MODIFY_REP\x10\xd9\x01\x12\x0c\n\x07SMM_PUB\x10\xda\x01\x12\x16\n\x11SMM_KEYPRICES_REQ\x10\xdb\x01\x12\x16\n\x11SMM_KEYPRICES_REP\x10\xdc\x01\x12\n\n\x05INDEX\x10\xdd\x01\x12\t\n\x04\x44\x44L1\x10\xde\x01\x12\x10\n\x0bGATEWAY_SUB\x10\xdf\x01\x12\x12\n\rGATEWAY_UNSUB\x10\xe0\x01\x12\x1d\n\x18\x42\x41SICDATA_MARKETSTYLEREQ\x10\xe1\x01\x12\x0e\n\tDDLSector\x10\xe2\x01\x12\x17\n\x12PORTFOLIO_EDIT_REQ\x10\xe3\x01\x12\x17\n\x12PORTFOLIO_EDIT_RSP\x10\xe4\x01\x12\x1b\n\x16PORTFOLIOITEM_EDIT_REQ\x10\xe5\x01\x12\x1b\n\x16PORTFOLIOITEM_EDIT_RSP\x10\xe6\x01\x12\x1a\n\x15\x41\x43\x43OUNT_AVAILABLE_REQ\x10\xe7\x01\x12\x1a\n\x15\x41\x43\x43OUNT_AVAILABLE_RSP\x10\xe8\x01\x12\x1a\n\x15\x41\x43\x43OUNT_AVAILABLE_PUB\x10\xe9\x01\x12\x19\n\x14USERAUTH_USEROPERATE\x10\xea\x01\x12\x1c\n\x17USERAUTH_PRODUCTOPERATE\x10\xeb\x01\x12\x17\n\x12RISK_RISKREPORTREQ\x10\xec\x01\x12\x17\n\x12RISK_RISKREPORTREP\x10\xed\x01\x12\t\n\x04\x44\x44RT\x10\xee\x01\x12$\n\x1fQUERY_SNAPLITESALL_COMPRESS_REQ\x10\xef\x01\x12$\n\x1fQUERY_SNAPLITESALL_COMPRESS_RSP\x10\xf0\x01\x12\x15\n\x10\x41NALYSIS_TCA_REQ\x10\xf1\x01\x12\x15\n\x10\x41NALYSIS_TCA_RSP\x10\xf2\x01\x12!\n\x1c\x46SLEND_QUERYSECURITYITEM_REQ\x10\xf3\x01\x12!\n\x1c\x46SLEND_QUERYSECURITYITEM_RSP\x10\xf4\x01\x12\x1d\n\x18\x46SLEND_QUERYCONTRACT_REQ\x10\xf5\x01\x12\x1d\n\x18\x46SLEND_QUERYCONTRACT_RSP\x10\xf6\x01\x12\x1f\n\x1aQUERYPOSITION_COMPRESS_REQ\x10\xf7\x01\x12\x1f\n\x1aQUERYPOSITION_COMPRESS_RSP\x10\xf8\x01\x12\x18\n\x13QUERYALLACCOUNT_REQ\x10\xf9\x01\x12\x18\n\x13QUERYALLACCOUNT_RSP\x10\xfa\x01\x12\x18\n\x13PUB_FSLEND_CONTRACT\x10\xfb\x01\x12\x16\n\x11LATENCYINDICATORS\x10\xfc\x01\x12\x1f\n\x1a\x43urrencyEtf_StrategyAddReq\x10\xfd\x01\x12 \n\x1b\x43urrencyEtf_StrategyAddResp\x10\xfe\x01\x12\x1f\n\x1a\x43urrencyEtf_StrategyAddPub\x10\xff\x01\x12\"\n\x1d\x43urrencyEtf_StrategyModifyReq\x10\x81\x02\x12#\n\x1e\x43urrencyEtf_StrategyModifyResp\x10\x82\x02\x12\"\n\x1d\x43urrencyEtf_StrategyModifyPub\x10\x83\x02\x12#\n\x1e\x43urrencyEtf_StrategyOperateReq\x10\x84\x02\x12$\n\x1f\x43urrencyEtf_StrategyOperateResp\x10\x85\x02\x12%\n CurrencyEtf_QueryStrategyListReq\x10\x86\x02\x12&\n!CurrencyEtf_QueryStrategyListResp\x10\x87\x02\x12)\n$CurrencyEtf_QueryOpenPositionListReq\x10\x88\x02\x12*\n%CurrencyEtf_QueryOpenPositionListResp\x10\x89\x02\x12)\n$CurrencyEtf_StrategyStatusChangedPub\x10\x8a\x02\x12+\n&CurrencyEtf_StrategyPositionChangedPub\x10\x8b\x02\x12\x16\n\x11QUERYACCTINFO_REQ\x10\x8c\x02\x12\x16\n\x11QUERYACCTINFO_RSP\x10\x8d\x02\x12#\n\x1eOPT_QUERYCOMSTRADETAILITEM_REQ\x10\x8e\x02\x12#\n\x1eOPT_QUERYCOMSTRADETAILITEM_RSP\x10\x8f\x02\x12\x1c\n\x17INSERTCOMBSTRAORDER_REQ\x10\x90\x02\x12\x1c\n\x17INSERTCOMBSTRAORDER_RSP\x10\x91\x02\x12\x15\n\x10OPT_BS_NORMS_PUB\x10\x92\x02\x12\x1f\n\x1aOPT_BS_DELTA_PORTFOLIO_PUB\x10\x93\x02\x12\x1b\n\x16OPT_BS_DELTA_HEDGE_PUB\x10\x94\x02\x12\x18\n\x13OPT_UNDL_MARKET_PUB\x10\x95\x02\x12\x19\n\x14\x41\x43\x43OUNT_NETVALUE_REQ\x10\x96\x02\x12\x19\n\x14\x41\x43\x43OUNT_NETVALUE_RSP\x10\x97\x02\x12#\n\x1eSTRATEGY_QUERYSTRATEGYLIST_REQ\x10\x98\x02\x12#\n\x1eSTRATEGY_QUERYSTRATEGYLIST_REP\x10\x99\x02\x12\x1a\n\x15STRATEGY_PUB_STRATEGY\x10\x9c\x02\x12\x1a\n\x15STRATEGY_PUB_POSITION\x10\x9d\x02\x12\x1d\n\x18STRATEGY_ADDSTRATEGY_REQ\x10\xa0\x02\x12\x1d\n\x18STRATEGY_ADDSTRATEGY_REP\x10\xa1\x02\x12\x1d\n\x18STRATEGY_DELSTRATEGY_REQ\x10\xa2\x02\x12\x1d\n\x18STRATEGY_DELSTRATEGY_REP\x10\xa3\x02\x12 \n\x1bSTRATEGY_MODIFYSTRATEGY_REQ\x10\xa4\x02\x12 \n\x1bSTRATEGY_MODIFYSTRATEGY_REP\x10\xa5\x02\x12!\n\x1cSTRATEGY_CONTROLSTRATEGY_REQ\x10\xa6\x02\x12!\n\x1cSTRATEGY_CONTROLSTRATEGY_REP\x10\xa7\x02\x12\x17\n\x12STRATEGY_PUB_TRADE\x10\xa8\x02\x12\x16\n\x11STRATEGY_RESERVED\x10\xb1\x02\x12\x1f\n\x1aSET_OPT_STARTEGY_STATE_REQ\x10\xb2\x02\x12\x1f\n\x1aSET_OPT_STARTEGY_STATE_RSP\x10\xb3\x02\x12\x1b\n\x16OPT_STARTEGY_STATE_PUB\x10\xb4\x02\x12\x1b\n\x16LFSIGNAL_ALPHABETA_PUB\x10\xb6\x02\x12\x19\n\x14OPT_BS_JOB_STATE_PUB\x10\xb7\x02\x12\x16\n\x11OPT_STRATEGYS_REQ\x10\xb8\x02\x12\x1c\n\x17OPT_COMBO_STRATEGYS_RSP\x10\xb9\x02\x12\x1c\n\x17OPT_HEDGE_STRATEGYS_RSP\x10\xba\x02\x12\x19\n\x14OPT_STRATEGY_DEL_REQ\x10\xbb\x02\x12\x19\n\x14OPT_STRATEGY_DEL_RSP\x10\xbc\x02\x12 \n\x1bOPT_COMBO_STRATEGYS_SET_RSP\x10\xbd\x02\x12 \n\x1bOPT_HEDGE_STRATEGYS_SET_RSP\x10\xbe\x02\x12 \n\x1bOPT_COMBO_STRATEGYS_SET_REQ\x10\xbf\x02\x12 \n\x1bOPT_HEDGE_STRATEGYS_SET_REQ\x10\xc0\x02\x12\n\n\x05ORDER\x10\xc1\x02\x12\x0f\n\nORDERQUEUE\x10\xc2\x02\x12)\n$CLIENT_QUERY_ORDER_BALANCE_PARAM_REQ\x10\xc3\x02\x12)\n$CLIENT_QUERY_ORDER_BALANCE_PARAM_RSP\x10\xc4\x02\x12\x19\n\x14TRANSFERINANDOUT_REQ\x10\xc5\x02\x12\x19\n\x14TRANSFERINANDOUT_RSP\x10\xc6\x02\x12\x16\n\x11QUERY_T0STOCK_REQ\x10\xc7\x02\x12\x16\n\x11QUERY_T0STOCK_RSP\x10\xc8\x02\x12\x15\n\x10SAVE_T0STOCK_REQ\x10\xc9\x02\x12\x15\n\x10SAVE_T0STOCK_RSP\x10\xca\x02\x12\x17\n\x12PUB_T0STOCK_CHANGE\x10\xcb\x02\x12\x16\n\x11\x41\x43\x43OUNT_IMPORINFO\x10\xcc\x02\x12\x19\n\x14\x41\x43\x43OUNT_IMPORINFORSP\x10\xcd\x02\x12\x11\n\x0cT0AGENT_AUTH\x10\xce\x02\x12\x14\n\x0fT0AGENT_AUTHREP\x10\xcf\x02\x12\x11\n\x0cT0USER_RIGHT\x10\xd0\x02\x12\x15\n\x10T0USER_RIGHT_REP\x10\xd1\x02\x12\x15\n\x10T0USER_RIGHT_PUB\x10\xd2\x02\x12\x1c\n\x17HFTRADING_THRESHOLD_REQ\x10\xd3\x02\x12\x1c\n\x17HFTRADING_THRESHOLD_REP\x10\xd4\x02\x12\x1c\n\x17HFTRADING_THRESHOLD_PUB\x10\xd5\x02\x12\x15\n\x10\x46\x45\x41TURE_INFERENC\x10\xd6\x02\x12\x12\n\rQUERYIPOQUOTA\x10\xd7\x02\x12\x15\n\x10QUERYIPOQUOTAREP\x10\xd8\x02\x12\x13\n\x0ePUB_RISKREASON\x10\xd9\x02\x12\x1a\n\x15JOBSTATUS_NOTIFY_SYNC\x10\xda\x02\x12\x15\n\x10\x41UTOTEST_CMD_REQ\x10\xa8\x46\x12\x15\n\x10\x41UTOTEST_CMD_REP\x10\xa9\x46\x12\"\n\x1d\x41UTOTEST_CMD_UPDATE_ASSET_REQ\x10\xaa\x46\x12\"\n\x1d\x41UTOTEST_CMD_UPDATE_ASSET_REP\x10\xab\x46\"\x1c\n\x0bMsgCarriers\x12\r\n\x05items\x18\x01 \x03(\x0c\"d\n\x05\x45rror\x12\x0b\n\x03msg\x18\x01 \x01(\x0c\x12\r\n\x05reqid\x18\x02 \x01(\x03\x12?\n\terrortype\x18\x03 \x01(\x0e\x32\x1d.PB.TYPEDEF.TypeDef.ErrorType:\rERROR_UNKNOWNP\x00')



_MSGCARRIER = DESCRIPTOR.message_types_by_name['MsgCarrier']
_MSGCARRIERS = DESCRIPTOR.message_types_by_name['MsgCarriers']
_ERROR = DESCRIPTOR.message_types_by_name['Error']
_MSGCARRIER_MSGTYPE = _MSGCARRIER.enum_types_by_name['MsgType']
MsgCarrier = _reflection.GeneratedProtocolMessageType('MsgCarrier', (_message.Message,), {
  'DESCRIPTOR' : _MSGCARRIER,
  '__module__' : 'msgcarrier_pb2'
  # @@protoc_insertion_point(class_scope:PB.MSGCARRIER.MsgCarrier)
  })
_sym_db.RegisterMessage(MsgCarrier)

MsgCarriers = _reflection.GeneratedProtocolMessageType('MsgCarriers', (_message.Message,), {
  'DESCRIPTOR' : _MSGCARRIERS,
  '__module__' : 'msgcarrier_pb2'
  # @@protoc_insertion_point(class_scope:PB.MSGCARRIER.MsgCarriers)
  })
_sym_db.RegisterMessage(MsgCarriers)

Error = _reflection.GeneratedProtocolMessageType('Error', (_message.Message,), {
  'DESCRIPTOR' : _ERROR,
  '__module__' : 'msgcarrier_pb2'
  # @@protoc_insertion_point(class_scope:PB.MSGCARRIER.Error)
  })
_sym_db.RegisterMessage(Error)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _MSGCARRIER._serialized_start=51
  _MSGCARRIER._serialized_end=8248
  _MSGCARRIER_MSGTYPE._serialized_start=307
  _MSGCARRIER_MSGTYPE._serialized_end=8248
  _MSGCARRIERS._serialized_start=8250
  _MSGCARRIERS._serialized_end=8278
  _ERROR._serialized_start=8280
  _ERROR._serialized_end=8380
# @@protoc_insertion_point(module_scope)
