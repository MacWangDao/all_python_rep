#include "limitorderbook.h"
#include "clog.h"
#include "math.h"

using namespace hy::influxdb;

char MATRICS[][10] = {"actbuy", "actsell", "actunk", "uptick", "downtick", "lvtick", "aop", "aoc", "bop", "boc"};
char DATATYPES[][4] = {"cnt", "vol", "amt"};
char SIZECLASS[][4] = {"sb", "bg", "md", "sm"};

Order::~Order()
{

}

int64_t Order::cancel_order(int64_t cancel_datetime, int64_t volume)
{
    m_left_volume -= volume;
    m_status = Status::CANCEL;
    m_last_update_time = cancel_datetime;
    return m_left_volume;
}

int64_t Order::confirm_transaction(int64_t trans_datetime, int64_t volume, double price)
{
    if (volume > m_left_volume)
        return -1;  // error! should not be larger than the left_volume

    if (m_status != Status::NEW && m_status != Status::PARTIAL)
        return -2; // error! only accept trans on NEW or PARTIALly filled orders.
    
    m_left_volume -= volume;
    m_status = Status::PARTIAL;
    if (m_left_volume <= 0)
        m_status = Status::COMPLETE;
    m_last_update_time = trans_datetime;
    return m_left_volume;
}

DynamicOrder::DynamicOrder(double price)
    : m_price(price)
    , m_timestamp(0)
    , m_trans_count(0)
    , m_trans_volume(0)
    , m_trans_amount(0.0)
{
    initialize();
}

DynamicOrder::~DynamicOrder()
{

}

void DynamicOrder::initialize()
{
    for (int i = 0; i < sizeof(MATRICS)/sizeof(MATRICS[0]); ++i){
        for(int m = 0; m < sizeof(DATATYPES)/sizeof(DATATYPES[0]); ++m) {
            std::string property_name = std::string(MATRICS[i]) + "_" + DATATYPES[m];
            if (strcmp(DATATYPES[m], "amt") == 0)
                m_data_amount[property_name] = 0.0;
            else
                m_data[property_name] = 0;

            for(int n = 0; n < sizeof(SIZECLASS)/sizeof(SIZECLASS[0]); ++n) {
                std::string property_name = std::string(MATRICS[i]) + "_" + DATATYPES[m] + "_" + SIZECLASS[n];
                if (strcmp(DATATYPES[m], "amt") == 0)
                    m_data_amount[property_name] = 0.0;
                else
                    m_data[property_name] = 0;
            }
        }
    }
}

std::vector<std::string> DynamicOrder::listdata_names()
{
    std::vector<std::string> vec_name;
    for (int i = 0; i < sizeof(MATRICS)/sizeof(MATRICS[0]); ++i){
        for(int m = 0; m < sizeof(DATATYPES)/sizeof(DATATYPES[0]); ++m) {
            std::string property_name = std::string(MATRICS[i]) + "_" + DATATYPES[m];
            vec_name.push_back(property_name);
            for(int n = 0; n < sizeof(SIZECLASS)/sizeof(SIZECLASS[0]); ++n) {
                std::string property_name = std::string(MATRICS[i]) + "_" + DATATYPES[m] + "_" + SIZECLASS[n];
                vec_name.push_back(property_name);
            }
        }
    }

    vec_name.push_back({"trans_cnt"});
    vec_name.push_back({"trans_vol"});
    vec_name.push_back({"trans_amt"});
    vec_name.push_back({"update_ts"});

    return vec_name;
}

std::string DynamicOrder::calc_sizeclass(int64_t volume, double amount)
{
    if (volume > 500000 || amount > 1000000.0)
        return "sb";
    else if (volume > 100000 || amount >= 200000.0)
        return "bg";
    else if (volume > 20000 || amount >= 40000.0)
        return "md";
    return "sm";
}

void DynamicOrder::put_order(int64_t order_datetime, int64_t volume, char nBSFlag)
{
    double amount = m_price * volume;
    std::string sizeclass = calc_sizeclass(volume, amount);
    std::string metric = nBSFlag == 'B' ? "bop" : "aop";
    m_data[(metric + "_cnt_" + sizeclass)] += 1;
    m_data[(metric + "_vol_" + sizeclass)] += volume;
    m_data_amount[(metric + "_amt_" + sizeclass)] += amount;

    m_data[(metric + "_cnt")] += 1;
    m_data[(metric + "_vol")] += volume;
    m_data_amount[(metric + "_amt")] += amount;
    m_timestamp = order_datetime;
}

void DynamicOrder::cancel_order(int64_t order_datetime, int64_t volume, char nBSFlag)
{
    double amount = m_price * volume;
    std::string sizeclass = calc_sizeclass(volume, amount);
    std::string metric = nBSFlag == 'B' ? "boc" : "aoc";

    m_data[(metric + "_cnt_" + sizeclass)] += 1;
    m_data[(metric + "_vol_" + sizeclass)] += volume;
    m_data_amount[(metric + "_amt_" + sizeclass)] += amount;

    m_data[(metric + "_cnt")] += 1;
    m_data[(metric + "_vol")] += volume;
    m_data_amount[(metric + "_amt")] += amount;
    m_timestamp = order_datetime;
}

void DynamicOrder::confirm_transaction(int64_t trans_datetime, int64_t volume, double amount, char bsflag, std::string tick_direction)
{
    std::string sizeclass = calc_sizeclass(volume, amount);
    std::string metric = "actunk";
    if (bsflag == 'B')
        metric = "actbuy";
    else if (bsflag == 'S')
        metric = "actsell";
    
    m_data[(metric + "_cnt_" + sizeclass)] += 1;
    m_data[(metric + "_vol_" + sizeclass)] += volume;
    m_data_amount[(metric + "_amt_" + sizeclass)] += amount;

    m_data[(metric + "_cnt")] += 1;
    m_data[(metric + "_vol")] += volume;
    m_data_amount[(metric + "_amt")] += amount;

    m_trans_count += 1;
    m_trans_volume += volume;
    m_trans_amount += amount;

    // up-down tick
    if (tick_direction == "uptick") {
        metric = "uptick";
    } else if (tick_direction == "downtick") {
        metric = "downtick";
    } else if (tick_direction == "leveltick") {
        metric = "lvtick";
    }

    m_data[(metric + "_cnt_" + sizeclass)] += 1;
    m_data[(metric + "_vol_" + sizeclass)] += volume;
    m_data_amount[(metric + "_amt_" + sizeclass)] += amount;

    m_data[(metric + "_cnt")] += 1;
    m_data[(metric + "_vol")] += volume;
    m_data_amount[(metric + "_amt")] += amount;

    m_timestamp = trans_datetime;
}

int64_t DynamicOrder::time()
{
    return m_timestamp;
}

double DynamicOrder::price()
{
    return m_price;
}

int64_t DynamicOrder::trans_count()
{
    return m_trans_count;
}

int64_t DynamicOrder::trans_volume()
{
    return m_trans_volume;
}

double DynamicOrder::trans_amount()
{
    return m_trans_amount;
}

int64_t DynamicOrder::get_value(const std::string& key)
{
    return m_data[key];
}

double DynamicOrder::get_amount(const std::string& key)
{
    return m_data_amount[key];
}

bool DynamicOrder::is_amt(const std::string& key)
{
    return key.find("amt") != std::string::npos;
}

LimitOrderBook::LimitOrderBook(const std::string& code, int exchange, int date)
    : m_code(code), m_exchange(exchange), m_date(date), m_last_order_time(0)
    , m_last_trans_time(0)
{
    if (m_exchange == 1)
        m_innercode = std::string("sh.") + m_code;
    else if (m_exchange == 2)
        m_innercode = std::string("sz.") + m_code;
    else
        m_innercode = m_code;
}

LimitOrderBook::~LimitOrderBook()
{
    m_all_orders.clear();
    m_order_on_prices.clear();
    m_dynamic_on_prices.clear();
}

int LimitOrderBook::date()
{
    return m_date;
}

std::string LimitOrderBook::code()
{
    return m_code;
}

std::string LimitOrderBook::innnercode()
{
    return m_innercode;
}

float LimitOrderBook::last_price()
{
    return m_last;
}

float LimitOrderBook::open_price()
{
    return m_open;
}

float LimitOrderBook::high_price()
{
    return m_high;
}

float LimitOrderBook::low_price()
{
    return m_low;
}

float LimitOrderBook::close_price()
{
    return m_close;
}

std::vector<double>& LimitOrderBook::bid_prices_queue()
{
    return m_bid_prices_queue_decr;
}

std::vector<double>& LimitOrderBook::ask_prices_queue()
{
    return m_ask_prices_queue_incr;
}

LimitOrderBook::price_order_map& LimitOrderBook::get_price_order_map()
{
    return m_order_on_prices;
}

LimitOrderBook::dynamic_price_order_map& LimitOrderBook::get_dynamic_order_map()
{
    return m_dynamic_on_prices;
}

void LimitOrderBook::on_data(PB::Quote::Order* order, PB::Quote::Transaction* trans)
{
    if (order != nullptr)
        on_order(order);
    
    // 逐笔成交可能存在未处理的数据，只要有数据到来都应执行transaction操作
    on_transaction(trans);
}

void LimitOrderBook::on_order(PB::Quote::Order* order)
{
    if (order->exchange() == PB::TYPEDEF::TypeDef_Exchange_EX_SH){
        on_order_sh(order);
    } else if (order->exchange() == PB::TYPEDEF::TypeDef_Exchange_EX_SZ){
        on_order_sz(order);
    }
}

void LimitOrderBook::on_transaction(PB::Quote::Transaction* trans)
{
    //优先处理缓存内数据
    if (m_cache_trans.size() > 0) {
        std::vector<PB::Quote::Transaction>::iterator iter;
        for (iter = m_cache_trans.begin(); iter != m_cache_trans.end();)
        {
            if ((*iter).exchange() == PB::TYPEDEF::TypeDef_Exchange_EX_SH){
                if (on_transaction_sh(&(*iter)))
                    iter = m_cache_trans.erase(iter);
                else 
                    ++iter;
            } else if ((*iter).exchange() == PB::TYPEDEF::TypeDef_Exchange_EX_SZ) {
                if (on_transaction_sz(&(*iter)))
                    iter = m_cache_trans.erase(iter);
                else
                    ++iter;
            }
        }
        this->update_prices_queue();
        // CLog::Write(INFO, " trans cache size: %ld", m_cache_trans.size());
    }

    if (trans != NULL)
    {
        //处理最新一笔数据，若新成交未与委托队列匹配则表示委托数据滞后，先将成交数据缓存，等下一笔数据到来再从缓存中取出处理
        if (trans->exchange() == PB::TYPEDEF::TypeDef_Exchange_EX_SH){
            if (!on_transaction_sh(trans)) {
                m_cache_trans.push_back(*trans);
            }
        } else if (trans->exchange() == PB::TYPEDEF::TypeDef_Exchange_EX_SZ) {
            if (!on_transaction_sz(trans)) {
                m_cache_trans.push_back(*trans);
            }
        }
    }
}

void LimitOrderBook::on_order_sh(PB::Quote::Order* order)
{
    m_last_order_time = order->time();
    int64_t datetime = to_timestamp(order->date(), order->time());
    uint64_t order_id = order->norderorino();  //原始订单号
    double price = order->nprice();
    if (order->chorderkind() == 'A') { //新增订单
        shared_ptr<Order> orderobj = make_shared<Order>(datetime, price, order->nvolume(), 
            order_id, order->chorderkind(), order->chfunctioncode());
        if (m_order_on_prices.find(price) == m_order_on_prices.end()) {
            m_order_on_prices[price] = {};
        }
        m_order_on_prices[price][order_id] = orderobj;
        m_all_orders[order_id] = orderobj;
        update_prices_queue();

        if (m_dynamic_on_prices.find(price) == m_dynamic_on_prices.end())
            m_dynamic_on_prices[price] = make_shared<DynamicOrder>(price);
        m_dynamic_on_prices[price]->put_order(datetime, order->nvolume(), order->chfunctioncode());

        // 集合竞价期间(9:15:00 - 9:25:00)使用，用于计算开盘价，仅计算委托数据，避免成交明细数据干扰
        if (m_last_order_time >= 33300000 && m_last_order_time <= 33900000) {
            best_price_map_add_order_sh(order);
        }
    } else if (order->chorderkind() == 'D') { //删除订单
        auto cancel = m_all_orders.find(order_id);
        if (cancel != m_all_orders.end()) {
            cancel->second->cancel_order(datetime, order->nvolume());
            m_dynamic_on_prices[cancel->second->m_price]->cancel_order(datetime, order->nvolume(), cancel->second->m_functioncode);
            if (m_last_order_time >= 33300000 && m_last_order_time <= 33900000)
                best_price_map_remove_order(cancel->second->m_price, order_id, order->nvolume());
            try_remove_order(cancel->second);  // 订单已完全成交或已撤单，尝试从委托队列中删除该订单
        }
    }
}

bool LimitOrderBook::on_transaction_sh(PB::Quote::Transaction* trans)
{
    m_last_trans_time = trans->time();
    int64_t datetime = to_timestamp(trans->date(), trans->time());
    double price = trans->lastprice();

    if (trans->chorderkind() == 0){ 
        bool trans_lead = m_last_trans_time > m_last_order_time;
        auto askorder_iter = m_all_orders.find(trans->naskorder());
        auto bidorder_iter = m_all_orders.find(trans->nbidorder());

        // BUG：沪市sp_ask_vol、sp_ask_amt、sp_bid_vol、sp_bid_amt等指标数据错误问题  2023-2-2
        // if (trans_lead && (askorder_iter == m_all_orders.end() || bidorder_iter == m_all_orders.end()))
        //     return false;
        // if (askorder_iter != m_all_orders.end()) {
        //     askorder_iter->second->confirm_transaction(datetime, trans->volume(), price);
        //     try_remove_order(askorder_iter->second);
        // }
        // if (bidorder_iter != m_all_orders.end()) {
        //     bidorder_iter->second->confirm_transaction(datetime, trans->volume(), price);
        //     try_remove_order(bidorder_iter->second);
        // }
        bool askIsNull = askorder_iter == m_all_orders.end();
        bool bidIsNull = bidorder_iter == m_all_orders.end();
        if (trans->nbsflag() == 'B') {
            if (askIsNull)
                return false;
            askorder_iter->second->confirm_transaction(datetime, trans->volume(), price);
            try_remove_order(askorder_iter->second);
        } else if (trans->nbsflag() == 'S') {
            if (bidIsNull)
                return false;
            bidorder_iter->second->confirm_transaction(datetime, trans->volume(), price);
            try_remove_order(bidorder_iter->second);
        } else if (trans->nbsflag() == ' ') {
            if (trans_lead && (askIsNull || bidIsNull))
                return false;
            if (!askIsNull) {
                askorder_iter->second->confirm_transaction(datetime, trans->volume(), price);
                try_remove_order(askorder_iter->second);
            }
            if (!bidIsNull) {
                bidorder_iter->second->confirm_transaction(datetime, trans->volume(), price);
                try_remove_order(bidorder_iter->second);
            }
        }

        auto dynamic_iter = m_dynamic_on_prices.find(price);
        if (dynamic_iter == m_dynamic_on_prices.end()) {
            m_dynamic_on_prices[price] = make_shared<DynamicOrder>(price);
        }
            
        m_dynamic_on_prices[price]->confirm_transaction(datetime, trans->volume(), trans->turnover(),
             trans->nbsflag(), get_tick_direction(price));

        update_ohlc(price);

        return true;
    }
    return false;
}

void LimitOrderBook::on_order_sz(PB::Quote::Order* order)
{
    m_last_order_time = order->time();

    int64_t datetime = to_timestamp(order->date(), order->time());
    uint64_t order_id = order->norder();
    double price = order->nprice();
    //获取价格，深市逐笔委托中包含对手方最优和本手方最优两种市价单类型，但订单价格为0或-1，订单的实际委托价格需要利用前序逐笔信息计算（逐笔成交）
    if ((order->chorderkind() == '1' || order->chorderkind() == 'U') &&
        (order->chfunctioncode() == 'B' || order->chfunctioncode() == 'S')) {
        double dprice = get_current_price(order->chorderkind(), order->chfunctioncode());
        price = fabs(dprice) > 0.00001 ? dprice : m_last;
    }

    //深市逐笔委托只有增量订单数据，撤单信息在逐笔成交中，本函数仅处理增量订单，撤单、成交在逐笔成交函数中
    shared_ptr<Order> orderobj = make_shared<Order>(datetime, price, order->nvolume(), order_id, order->chorderkind(), order->chfunctioncode());
    if (m_order_on_prices.find(price) == m_order_on_prices.end()) {
        m_order_on_prices[price] = {};
    } 
    m_order_on_prices[price][order_id] = orderobj;
    m_all_orders[order_id] = orderobj;
    update_prices_queue();

    if (m_dynamic_on_prices.find(price) == m_dynamic_on_prices.end())
        m_dynamic_on_prices[price] = make_shared<DynamicOrder>(price);
    m_dynamic_on_prices[price]->put_order(datetime, order->nvolume(), order->chfunctioncode());

    // 集合竞价期间(9:15:00 - 9:25:00)使用，用于计算开盘价，仅计算委托数据，避免成交明细数据干扰
    if (m_last_order_time >= 33300000 && m_last_order_time <= 33900000) {
        best_price_map_add_order_sz(order);
    }
}

bool LimitOrderBook::on_transaction_sz(PB::Quote::Transaction* trans)
{
    m_last_trans_time = trans->time();

    double price = trans->lastprice();
    int32_t askorder = trans->naskorder();
    int32_t bidorder = trans->nbidorder();
    int64_t datetime = to_timestamp(trans->date(), trans->time());

    if (trans->chfunctioncode() == '0') //成交
    {
        auto askorder_iter = m_all_orders.find(askorder); //卖单委托队列
        auto bidorder_iter = m_all_orders.find(bidorder); //买单委托队列

        if (askorder_iter != m_all_orders.end() && bidorder_iter != m_all_orders.end()) {
            askorder_iter->second->confirm_transaction(datetime, trans->volume(), price);
            try_remove_order(askorder_iter->second);

            bidorder_iter->second->confirm_transaction(datetime, trans->volume(), price);
            try_remove_order(bidorder_iter->second);
            //confirm DOP
            auto dynamic_iter = m_dynamic_on_prices.find(price);
            if (dynamic_iter == m_dynamic_on_prices.end())
                m_dynamic_on_prices[price] = make_shared<DynamicOrder>(price);
            m_dynamic_on_prices[price]->confirm_transaction(datetime, trans->volume(), trans->turnover(), 
                trans->nbsflag(), get_tick_direction(price));
        
            update_ohlc(price);
        
            return true;
        } else {
            return false;
        }
    }
    else if (trans->chfunctioncode() == 'C') //取消
    {
        auto cancel = m_all_orders.find(askorder != 0 ? askorder : bidorder);
        if (cancel != m_all_orders.end()) {
            cancel->second->cancel_order(datetime, trans->volume());
            m_dynamic_on_prices[cancel->second->m_price]->cancel_order(datetime, trans->volume(), cancel->second->m_functioncode);
            if (m_last_trans_time >= 33300000 && m_last_trans_time <= 33900000)
                best_price_map_remove_order(cancel->second->m_price, cancel->second->m_order_no, trans->volume());
            try_remove_order(cancel->second);
            return true;
        } else {
            return false;
        }
    }
    return false;
}
void LimitOrderBook::try_remove_order(shared_ptr<Order> shared_order_obj)
{
    double price = shared_order_obj->m_price;
    uint64_t order_id = shared_order_obj->m_order_no;
    if ((shared_order_obj->m_status == Order::COMPLETE
        || shared_order_obj->m_status == Order::CANCEL) 
        && shared_order_obj->m_left_volume <= 0)
    {
        auto iter = m_all_orders.find(order_id);
        if (iter != m_all_orders.end())
            m_all_orders.erase(iter);         // 从全局委托队列中删除指定订单号委托数据

        auto queue_iter = m_order_on_prices.find(price);
        if ( queue_iter != m_order_on_prices.end() ) {
            auto iter_order = queue_iter->second.find(order_id);
            if ( iter_order != queue_iter->second.end())
                queue_iter->second.erase(iter_order);  // 从委托队列中删除指定订单号数据
        }

        if ( queue_iter->second.size() == 0) {  
            m_order_on_prices.erase(queue_iter); // 队列为空时将该队列从价格队列中清除
            update_prices_queue();
        }

        //CLog::Write(INFO, "LimitOrderBook::try_remove_order: %d", order_id);
    }
}
// 新增订单
void LimitOrderBook::best_price_map_add_order_sh(PB::Quote::Order* order)
{
    double price = order->nprice();
    if (m_best_price_map.find(price) == m_best_price_map.end())
        m_best_price_map[price] = {};
    uint64_t order_id = order->norderorino();  //原始订单号
    shared_ptr<Order> orderobj = make_shared<Order>(to_timestamp(order->date(), order->time()), 
        price, order->nvolume(), order_id, order->chorderkind(), order->chfunctioncode());
    m_best_price_map[price][order_id] = orderobj;
}

void LimitOrderBook::best_price_map_add_order_sz(PB::Quote::Order* order)
{
    double price = order->nprice();
    if (m_best_price_map.find(price) == m_best_price_map.end())
        m_best_price_map[price] = {};
    uint64_t order_id = order->norder();
    shared_ptr<Order> orderobj = make_shared<Order>(to_timestamp(order->date(), order->time()), 
        price, order->nvolume(), order_id, order->chorderkind(), order->chfunctioncode());
    m_best_price_map[price][order_id] = orderobj;
}
// 删除订单
void LimitOrderBook::best_price_map_remove_order(double price, uint64_t order_id, int64_t volume)
{
    auto map_iter = m_best_price_map.find(price);
    if (map_iter != m_best_price_map.end()) {
        auto order_iter = map_iter->second.find(order_id);
        if (order_iter != map_iter->second.end()) {
            order_iter->second->cancel_order(0, volume);
            if ((order_iter->second->m_status == Order::COMPLETE  || order_iter->second->m_status == Order::CANCEL) 
                && order_iter->second->m_left_volume <= 0){
                map_iter->second.erase(order_iter);
            }
        }
        if (map_iter->second.empty()) {
            m_best_price_map.erase(map_iter);
        }
    }
}

uint64_t LimitOrderBook::best_calc_left_volume_on_price(double price, char bs_flag)
{
    uint64_t volume = 0;
    for (auto order : m_best_price_map[price]) {
        if (order.second->m_functioncode == bs_flag) {
            volume += order.second->m_left_volume;
        }
    }
    return volume;
}

double LimitOrderBook::get_current_price(int kind, int bsflag)
{
    if (kind == '1' && bsflag == 'B')             //买单，对方最优
        return m_ask_prices_queue_incr[0];
    else if (kind == 'U' && bsflag == 'B')      //买单，本方最优
        return m_bid_prices_queue_decr[0];
    else if (kind == '1' && bsflag == 'S')        //卖单，对方最优
        return m_bid_prices_queue_decr[0];
    else if (kind == 'U' && bsflag == 'S')      //卖单，本方最优
        return m_ask_prices_queue_incr[0];
    return 0.0;
}

bool cmp_value(const pair<double, int64_t> left, const pair<double, int64_t> right){
    return left.second < right.second;
}

void LimitOrderBook::match_best_price(double lclose_price)
{
    m_last = lclose_price;
    if (!m_best_price_map.empty())
    {
        std::map<double, int64_t> bid_vol_dict = {};
        std::map<double, int64_t> ask_vol_dict = {};
        std::map<double, int64_t> min_price_dict = {};
        std::map<double, int64_t> min_unsettled_volume_dict = {}; //未成交量

        std::vector<double> all_prices;
        for (auto iter : m_best_price_map) {
            all_prices.push_back(iter.first);
        }

        for (double base_price : all_prices)
        {
            int64_t bid_vol_value_sum = 0;
            int64_t ask_vol_value_sum = 0;
            for (double price : all_prices) {
                // 指定买价及以上买价格所有量累加
                if (price >= base_price) {
                    int64_t volume = best_calc_left_volume_on_price(price, 'B');
                    if (volume > 0) {
                        bid_vol_value_sum += volume;  //买量累加
                        bid_vol_dict[base_price] = bid_vol_value_sum;
                    }
                }

                // 指定卖价及以下卖价格所有量累加
                if (price <= base_price) {
                    int64_t volume = best_calc_left_volume_on_price(price, 'S');
                    if (volume > 0) {
                        ask_vol_value_sum += volume;
                        ask_vol_dict[base_price] = ask_vol_value_sum;
                    }
                }
            }

            min_price_dict[base_price] = std::min(bid_vol_value_sum, ask_vol_value_sum);
        }
        
        if (min_price_dict.size() > 0) {
            int64_t vol_value_max = max_element(min_price_dict.begin(), min_price_dict.end(), cmp_value)->second;
            std::vector<double> multiple_values;
            for(auto iter : min_price_dict) {
                if (vol_value_max == iter.second) {
                    multiple_values.push_back(iter.first);
                    min_unsettled_volume_dict[iter.first] = labs(ask_vol_dict[iter.first] - bid_vol_dict[iter.first]);
                    // CLog::Write(INFO, " %.2f, %ld %ld", iter.first, iter.second, min_unsettled_volume_dict[iter.first]);
                }
            }

            // 《上海证券交易所交易规则》、《深圳证券交易所交易规则》，集合竞价时，成交价的确定原则：
            if (multiple_values.size() > 1) {
                multiple_values.clear();
                int64_t min_volume = min_element(min_unsettled_volume_dict.begin(), min_unsettled_volume_dict.end(), cmp_value)->second;
                for (auto iter : min_unsettled_volume_dict) {
                    if (min_volume == iter.second) 
                        multiple_values.push_back(iter.first);
                }
            }
            if (vol_value_max != 0) {
                if (m_exchange == 1) {
                    if (multiple_values.size() % 2 != 0) //奇数个
                        m_last = multiple_values[multiple_values.size()/2];
                    else {
                        m_last = (multiple_values[multiple_values.size()/2] + multiple_values[multiple_values.size()/2 -1]) / 2.0;
                        // m_last = round(m_last, 2);
                    }
                } else if (m_exchange == 2) {
                    m_last = eva_proximity(multiple_values, lclose_price); // 深圳取与昨收价最接近的值
                }
            }
        }
    }

    // CLog::Write(INFO, "best_price open: %.2f", m_last);
}

void LimitOrderBook::update_ohlc(double lastprice)
{
    m_last = lastprice;
    if (m_last > m_high)
        m_high = m_last;
    if (m_last < m_low)
        m_low = m_last;
    m_close = m_last;
     
    //CLog::Write(INFO, "update_ohlc %.2f %.2f %.2f %.2f", m_last, m_high, m_low, m_close);
}

void LimitOrderBook::new_ohlc_bar()
{
    m_open = m_last;
    m_high = m_last;
    m_low = m_last;
    m_close = m_last;
}

std::string doubleToString(const double &value)
{
    char sz[40] = {0};
    sprintf(sz, "%.2f", value);
    return sz;
}

void vectorToString(const std::vector<double>& vec, std::string& value, char separator = ',')
{
    for (double val : vec){
        value.append(std::string{separator}).append(doubleToString(val));
    }
}

std::string LimitOrderBook::bid_price_queue_str()
{
    std::string str;
    for (double price : m_bid_prices_queue_decr)
    {
        str += doubleToString(price);
        str += ",";
    }
    if (!str.empty())
        str = str.substr(0, str.length()-1);
    return str;
}

std::string LimitOrderBook::ask_price_queue_str()
{
    std::string str;
    for (double price : m_ask_prices_queue_incr)
    {
        str += doubleToString(price);
        str += ",";
    }
    if (!str.empty())
        str = str.substr(0, str.length()-1);
    return str;
}

uint64_t LimitOrderBook::calc_left_volume_on_price(double price, char bs_flag)
{
    uint64_t left_volume = 0;
    auto iter = m_order_on_prices.find(price);
    if (iter != m_order_on_prices.end()) {
        for (auto order : iter->second) {
            if (bs_flag == order.second->m_functioncode) {
                left_volume += order.second->m_left_volume;
            }
        }
    }
    return left_volume;
}

bool LimitOrderBook::have_left_volume_on_price(double price, char bs_flag)
{
    auto iter = m_order_on_prices.find(price);
    if (iter != m_order_on_prices.end()) {
        for (auto iter : iter->second) {
            if (bs_flag == iter.second->m_functioncode && iter.second->m_left_volume > 0)
                return true;
        }
    }
    return false;
}

void LimitOrderBook::update_prices_queue()
{
    m_bid_prices_queue_decr.clear();
    m_ask_prices_queue_incr.clear();

    for(auto iter = m_order_on_prices.begin(); iter != m_order_on_prices.end(); ++iter)
    {
        double price = iter->first;
        
        if (fabs(price - m_last) <= 0.000001 || price < m_last){ //买价
            if (have_left_volume_on_price(price, 'B'))
                m_bid_prices_queue_decr.push_back(price);
            else
                m_bid_prices_queue_decr.push_back(m_last);
        }

        if (fabs(price - m_last) <= 0.000001 || price > m_last){ //卖价
            if (have_left_volume_on_price(price, 'S'))
                m_ask_prices_queue_incr.push_back(price);
            else 
                m_ask_prices_queue_incr.push_back(m_last);
        }
    }

    sort(m_bid_prices_queue_decr.begin(), m_bid_prices_queue_decr.end(), greater<double>());
    sort(m_ask_prices_queue_incr.begin(), m_ask_prices_queue_incr.end(), less<double>());
}

std::string LimitOrderBook::get_tick_direction(double new_price)
{
    if (fabs(new_price - m_last) <= 0.000001)
        return "leveltick"; 
    else if (new_price > m_last)
        return "uptick";
    else if (new_price < m_last)
        return "downtick";
    return "";
}

bool is_hour_between(int time, int start_time, int end_time)
{
    return time >= start_time && time <= end_time;
}

void calc_snapshot_timestamp_series(std::vector<int>& series, int start_time, int end_time, int64_t time_interval)
{
    int time_node = (end_time/1000)*1000;
    if (start_time <= 0) {
        series.push_back(time_node);
        return;
    }

    if (time_node < start_time) {
        if (start_time >= 33300000 && start_time <= 33900000) {
            series.push_back(time_node);
        }
        return;
    }

    if (end_time > start_time) {
        int time_node = (start_time/1000) * 1000;  //时间节点
        end_time = (end_time/1000) * 1000;    //结束时间

        int timespan = end_time - time_node;
        for (int i=0; i < timespan + 1000 - time_interval; i += time_interval) {
            time_node += time_interval;
            if ((time_node >= 33903000 && time_node <= 34197000) || (time_node >= 41403000 && time_node <= 46797000)) // 09:25:03 ~ 09:29:57  11:30:03 ~ 12:59:57
                continue;
            series.push_back(time_node);
        }
    }
}

void calc_ask_bid_volume_amount_by_prices( std::shared_ptr<LimitOrderBook> lob,
    std::map<double, int64_t>& asks_volume, std::map<double, double>& asks_amount,
    std::map<double, int64_t>& bids_volume, std::map<double, double>& bids_amount,
    std::vector<double>& vec_price)
{
    //计算每档价格 的量 和 金额
    auto pirces_order = lob->get_price_order_map();
    for (auto iter : pirces_order)
    {
        double price = iter.first;
        int64_t askvolume = 0;
        int64_t bidvolume = 0;
        for (auto iter_order : iter.second) {
            if (iter_order.second->m_functioncode == 'S')
                askvolume += iter_order.second->m_left_volume;
            if (iter_order.second->m_functioncode == 'B')
                bidvolume += iter_order.second->m_left_volume;
        }

        asks_volume[price] = askvolume;
        asks_amount[price] = askvolume * price;
        bids_volume[price] = bidvolume;
        bids_amount[price] = bidvolume * price;

        vec_price.push_back(price);
    }
}

void get_points_by_techs(std::shared_ptr<LimitOrderBook> lob, int64_t snapshot_ts, std::vector<hy::influxdb::Point>& points)
{
    // 证券代码（例：sh.600001)
    std::string code = lob->innnercode();
    // 高开低收，买卖价格队列
    points.push_back(Point{"sl_last_price"}.addTag("instrument", code).addField("value", lob->last_price()).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_open_price"}.addTag("instrument", code).addField("value", lob->open_price()).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_high_price"}.addTag("instrument", code).addField("value", lob->high_price()).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_low_price"}.addTag("instrument", code).addField("value", lob->low_price()).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_close_price"}.addTag("instrument", code).addField("value", lob->close_price()).setTimestamp(snapshot_ts));

    // 价格队列
    points.push_back(Point{"sl_ask_prices"}.addTag("instrument", code).addField("value", lob->ask_price_queue_str()).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_ask_prices_depth"}.addTag("instrument", code).addField("value", (int)lob->ask_prices_queue().size()).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_bid_prices"}.addTag("instrument", code).addField("value", lob->bid_price_queue_str()).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_bid_prices_depth"}.addTag("instrument", code).addField("value", (int)lob->bid_prices_queue().size()).setTimestamp(snapshot_ts));

    // 技术指标
    std::map<double, int64_t> asks_volume;  // 价格 -> 量(卖)
    std::map<double, int64_t> bids_volume;  // 价格 -> 量(买)
    std::map<double, double>  asks_amount;  // 价格 -> 金额(卖)
    std::map<double, double>  bids_amount;  // 价格 -> 金额(买)
    std::vector<double> vec_price;
    calc_ask_bid_volume_amount_by_prices(lob, asks_volume, asks_amount, bids_volume, bids_amount, vec_price);

    // 量累加、金额累加
    // sl_ask_vol、sl_bid_vol、sl_ask_amt、sl_bid_amt
    int64_t ask_vols = 0;
    for(auto iter : asks_volume)
        ask_vols += iter.second;
    int64_t bid_vols = 0;
    for (auto iter : bids_volume)
        bid_vols += iter.second;
    double ask_amt = 0.0;
    for (auto iter : asks_amount)
        ask_amt += iter.second;
    double bid_amt = 0.0;
    for (auto iter : bids_amount)
        bid_amt += iter.second;
    points.push_back(Point{"sl_ask_vol"}.addTag("instrument", code).addField("value", ask_vols).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_bid_vol"}.addTag("instrument", code).addField("value", bid_vols).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_ask_amt"}.addTag("instrument", code).addField("value", ask_amt).setTimestamp(snapshot_ts));
    points.push_back(Point{"sl_bid_amt"}.addTag("instrument", code).addField("value", bid_amt).setTimestamp(snapshot_ts));


    for (auto key : vec_price) {
        std::string price = doubleToString(key);
        points.push_back(Point{"sp_ask_vol"}.addTag("instrument", code).addTag("price", price).addField("value", asks_volume[key]).setTimestamp(snapshot_ts));
        points.push_back(Point{"sp_bid_vol"}.addTag("instrument", code).addTag("price", price).addField("value", bids_volume[key]).setTimestamp(snapshot_ts));
        points.push_back(Point{"sp_ask_amt"}.addTag("instrument", code).addTag("price", price).addField("value", asks_amount[key]).setTimestamp(snapshot_ts));
        points.push_back(Point{"sp_bid_amt"}.addTag("instrument", code).addTag("price", price).addField("value", bids_amount[key]).setTimestamp(snapshot_ts));
    }

    std::vector<std::string> int_keys, float_keys;
    std::map<std::string, int64_t> tech_vols;
    std::map<std::string, double>  tech_amts;
    for (int i = 0; i < sizeof(MATRICS)/sizeof(MATRICS[0]); ++i){
        for(int m = 0; m < sizeof(DATATYPES)/sizeof(DATATYPES[0]); ++m) {
             std::string key = std::string(MATRICS[i]) + "_" + DATATYPES[m];
            if (strcmp(DATATYPES[m], "vol") == 0 || strcmp(DATATYPES[m], "cnt") == 0) {
                int_keys.push_back(key);
                tech_vols[key] = 0;
            }
            if (strcmp(DATATYPES[m], "amt") == 0) {
                float_keys.push_back(key);
                tech_amts[key] = 0.0;
            }

            for(int n = 0; n < sizeof(SIZECLASS)/sizeof(SIZECLASS[0]); ++n) {
                std::string key = std::string(MATRICS[i]) + "_" + DATATYPES[m] + "_" + SIZECLASS[n];
                if (strcmp(DATATYPES[m], "vol") == 0 || strcmp(DATATYPES[m], "cnt") == 0) {
                    int_keys.push_back(key);
                    tech_vols[key] = 0;
                }
                if (strcmp(DATATYPES[m], "amt") == 0) {
                    float_keys.push_back(key);
                    tech_amts[key] = 0.0;
                }
            }
        }
    }

    auto dynamicOrders = lob->get_dynamic_order_map();
    for (auto iter : dynamicOrders) {
        for (auto key : int_keys)
            tech_vols[key] += iter.second->get_value(key);
        for (auto key : float_keys)
            tech_amts[key] += iter.second->get_amount(key);
    }

    for (auto key : int_keys) {
        std::string measurement = std::string("sl_").append(key);
        points.push_back(Point(measurement).addTag("instrument", code).addField("value", tech_vols[key]).setTimestamp(snapshot_ts));
    }

    for (auto key : float_keys) {
        std::string measurement = std::string("sl_").append(key);
        points.push_back(Point(measurement).addTag("instrument", code).addField("value", tech_amts[key]).setTimestamp(snapshot_ts));
    }
}

void lineProtocol(std::string& line, const std::string& measurement, const std::string& instrument, double value, int64_t& time)
{
    line += measurement;
    line += ",";
    line += "instrument=";
    line += instrument;
    line += " value=";
    line += to_string(value);
    line += " ";
    line += to_string(time);
    line += "\n";
}

void lineProtocol2(std::string& line, const std::string& measurement, const std::string& instrument, int64_t& value, int64_t& time)
{
    line += measurement;
    line += ",";
    line += "instrument=";
    line += instrument;
    line += " value=";
    line += to_string(value);
    line += " ";
    line += to_string(time);
}

std::string get_points_by_techs(std::shared_ptr<LimitOrderBook> lob, int64_t snapshot_ts)
{
    std::string line = "";
    // 证券代码（例：sh.600001)
    std::string code = lob->code();
    // 高开低收，买卖价格队列    
    lineProtocol(line, std::string("sl_last_price"), code, lob->last_price(), snapshot_ts);
    lineProtocol(line, std::string("sl_open_price"), code, lob->open_price(), snapshot_ts);
    lineProtocol(line, std::string("sl_high_price"), code, lob->high_price(), snapshot_ts);
    lineProtocol(line, std::string("sl_low_price"), code, lob->low_price(), snapshot_ts);
    lineProtocol(line, std::string("sl_close_price"), code, lob->close_price(), snapshot_ts);
    return line;
}