<script setup lang="ts">
import { ref, onMounted, onUnmounted } from "vue";
import {
  Compass,
  MapPin,
  BookOpen,
  Ticket,
  UserRound,
  ArrowUpRight,
  Search,
  SlidersHorizontal,
  Check,
  Store,
  CalendarDays,
  Heart,
  PanelLeft,
  ScanLine,
  Layers,
} from "lucide-vue-next";
import { call, distance, statuses, RequestError } from "./api";
type Hit = { id: number; name: string; distanceMeters: number; typeId: number };
type Detail = {
  id: number;
  name: string;
  address: string;
  score: number;
  open_hours: string;
};
type Slot = { id: number; startsAt: string; remaining: number };
type Voucher = {
  id: number;
  title: string;
  stock: number | null;
  type: number;
  pay_value: number;
  actual_value: number;
};
type Order = { id: string; title: string; status: number };
type Note = {
  id: number;
  title: string;
  content: string;
  author: string;
  liked: number;
};
type Reward = {
  days: number;
  daysRequired: number;
  claimed: boolean;
  signed: boolean;
  streak: number;
};
const tabs = [
  { name: "附近商铺", icon: Compass },
  { name: "优惠与订单", icon: Ticket },
  { name: "探店笔记", icon: BookOpen },
  { name: "个人中心", icon: UserRound },
];
const tab = ref(0),
  menu = ref(false),
  busy = ref(false),
  message = ref(""),
  failed = ref(false),
  filters = ref(false);
const query = ref(""),
  category = ref(0),
  x = ref(120.15),
  y = ref(30.25),
  radius = ref(5000),
  shops = ref<Hit[]>([]),
  categories = ref<{ id: number; name: string }[]>([]),
  detail = ref<Detail>(),
  slots = ref<Slot[]>([]),
  party = ref(1);
const token = ref(localStorage.getItem("xzdp.token") || ""),
  account = ref(""),
  code = ref(""),
  reward = ref<Reward>();
const vouchers = ref<Voucher[]>([]),
  orders = ref<Order[]>([]),
  pending = ref(""),
  redeemID = ref(""),
  credential = ref(""),
  receipt = ref("");
const notes = ref<Note[]>([]),
  title = ref(""),
  content = ref(""),
  shopID = ref(1);
let poll: ReturnType<typeof setInterval> | undefined;
async function act(fn: () => Promise<void>) {
  if (busy.value) return;
  busy.value = true;
  message.value = "";
  failed.value = false;
  try {
    await fn();
  } catch (e) {
    if (e instanceof RequestError && e.status === 401) {
      clearSession();
      tab.value = 3;
    }
    failed.value = true;
    message.value = e instanceof Error ? e.message : String(e);
  } finally {
    busy.value = false;
  }
}
async function search() {
  const params = new URLSearchParams({
    x: String(x.value),
    y: String(y.value),
    radiusMeters: String(radius.value),
    q: query.value,
    typeId: String(category.value),
  });
  shops.value = await call<Hit[]>("/shops/search?" + params);
  detail.value = undefined;
}
async function open(id: number) {
  const [d, s] = await Promise.all([
    call<Detail>("/shops/" + id),
    call<Slot[]>("/shops/" + id + "/slots"),
  ]);
  detail.value = d;
  slots.value = s;
}
async function refreshOrders() {
  if (token.value) orders.value = await call<Order[]>("/orders");
}
async function load() {
  if (tab.value === 0) {
    categories.value = await call("/categories");
    await search();
  }
  if (tab.value === 1) {
    vouchers.value = await call("/vouchers");
    await refreshOrders();
  }
  if (tab.value === 2) notes.value = await call("/notes");
  if (tab.value === 3 && token.value) {
    [reward.value, bookings.value, wallet.value] = await Promise.all([
      call<Reward>("/user/sign/reward"),
      call<typeof bookings.value>("/bookings"),
      call<typeof wallet.value>("/wallet"),
    ]);
  }
}
async function navigate(i: number) {
  if (busy.value) return;
  tab.value = i;
  menu.value = false;
  await act(load);
}
function auth() {
  if (token.value) return true;
  tab.value = 3;
  message.value = "请登录后继续";
  return false;
}
async function send() {
  const v = await call<{ demoCode?: string } | string>("/user/email/code", {
    email: account.value,
    purpose: "login",
  });
  if (typeof v === "object" && v.demoCode) {
    code.value = v.demoCode;
    message.value = "演示验证码已填入";
  } else message.value = "验证码已发送";
}
async function login() {
  token.value = await call("/user/email/login", {
    email: account.value,
    code: code.value,
  });
  localStorage.setItem("xzdp.token", token.value);
  code.value = "";
  await load();
}
async function buy(v: Voucher) {
  if (!auth()) return;
  const id = await call<string>("/vouchers/" + v.id + "/buy", {});
  if (v.type) {
    pending.value = id;
    message.value = "抢券已受理";
  } else message.value = "优惠券已领取";
  await refreshOrders();
}
let polling = false;
async function tick() {
  if (!token.value || busy.value || tab.value !== 1 || polling) return;
  polling = true;
  if (!pending.value) {
    try {
      await refreshOrders();
    } catch (e) {
      message.value = String(e);
      failed.value = true;
    } finally {
      polling = false;
    }
    return;
  }
  try {
    const state = await call<string>("/orders/" + pending.value);
    if (state !== "queued") {
      await refreshOrders();
      pending.value = "";
    }
  } catch (e) {
    message.value = String(e);
    failed.value = true;
  } finally {
    polling = false;
  }
}
async function pay(id: string) {
  const result = await call<{ code: string }>("/orders/" + id + "/pay", {});
  receipt.value = result.code;
  await refreshOrders();
}
async function publish() {
  if (!auth()) return;
  await call("/notes", {
    title: title.value,
    content: content.value,
    shopID: shopID.value,
  });
  content.value = "";
  title.value = "";
  await load();
}
const wallet = ref<{ title: string; source: string }[]>([]);
const bookings = ref<
  { id: number; slotId: number; partySize: number; cancelled: boolean }[]
>([]);
const keys = new Map<string, string>();
async function reserve(id: number) {
  if (!auth()) return;
  const k = id + "-" + party.value;
  const key = keys.get(k) || crypto.randomUUID();
  keys.set(k, key);
  const b = await call<(typeof bookings.value)[number]>(
    "/slots/" + id + "/bookings",
    { partySize: party.value },
    { "Idempotency-Key": key },
  );
  keys.delete(k);
  bookings.value.push(b);
  await open(detail.value!.id);
  message.value = "已预约 " + b.partySize + " 位";
}
async function logout() {
  try {
    await call("/user/logout", {});
  } finally {
    clearSession();
  }
}
function clearSession() {
  wallet.value = [];
  code.value = "";
  account.value = "";
  credential.value = "";
  redeemID.value = "";
  token.value = "";
  localStorage.removeItem("xzdp.token");
  orders.value = [];
  receipt.value = "";
  bookings.value = [];
  pending.value = "";
  reward.value = undefined;
}
onMounted(() => {
  void act(load);
  poll = setInterval(tick, 1200);
});
onUnmounted(() => clearInterval(poll));
</script>
<template>
  <div class="shell">
    <aside :class="{ expanded: menu }">
      <div class="brand">
        <span class="brand-icon"><Layers :size="20" /></span
        ><strong>小众点评</strong>
      </div>
      <span class="nav-caption">工作台</span>
      <nav>
        <button
          v-for="(item, i) in tabs"
          :class="{ active: tab === i }"
          @click="navigate(i)"
          :disabled="busy"
        >
          <component :is="item.icon" :size="17" />{{ item.name }}
        </button>
      </nav>
      <div class="side-foot"><span class="dot"></span>城市体验台</div>
    </aside>
    <main>
      <header>
        <button class="mobile" aria-label="导航" @click="menu = !menu">
          <PanelLeft :size="18" /></button
        ><span>工作台</span><span>/</span><b>{{ tabs[tab].name }}</b
        ><button class="account" @click="navigate(3)">
          <UserRound :size="15" />{{ token ? "我的账户" : "登录" }}
        </button>
      </header>
      <div class="page">
        <div class="heading">
          <component :is="tabs[tab].icon" :size="25" />
          <h1>{{ tabs[tab].name }}</h1>
        </div>
        <div
          v-if="message"
          role="status"
          :class="['notice', { error: failed }]"
        >
          {{ message }}
        </div>
        <template v-if="tab === 0"
          ><form class="toolbar" @submit.prevent="act(search)">
            <label class="search"
              ><Search :size="17" /><input
                v-model="query"
                placeholder="搜索店名或关键词"
                aria-label="店名或关键词" /></label
            ><button type="button" @click="filters = !filters">
              <SlidersHorizontal :size="15" />筛选</button
            ><button class="primary" :disabled="busy">搜索</button>
          </form>
          <form
            v-if="filters"
            class="panel fields"
            @submit.prevent="act(search)"
          >
            <label
              >分类<select v-model="category">
                <option :value="0">全部分类</option>
                <option v-for="c in categories" :value="c.id">
                  {{ c.name }}
                </option>
              </select></label
            ><label
              >经度<input
                v-model.number="x"
                type="number"
                step="any"
                required /></label
            ><label
              >纬度<input
                v-model.number="y"
                type="number"
                step="any"
                required /></label
            ><label
              >半径（米）<input
                v-model.number="radius"
                type="number"
                min="1"
                max="20000000" /></label
            ><button>应用</button>
          </form>
          <div class="columns">
            <section class="panel">
              <div class="section-title">
                <h2>搜索结果</h2>
                <small>{{ shops.length }} 家商铺</small>
              </div>
              <button
                v-for="s in shops"
                class="row"
                @click="act(() => open(s.id))"
              >
                <span class="tile"><Store :size="21" /></span
                ><span class="grow"
                  ><strong>{{ s.name }}</strong
                  ><small>{{
                    categories.find((c) => c.id === s.typeId)?.name
                  }}</small></span
                ><span class="mono">{{ distance(s.distanceMeters) }}</span
                ><ArrowUpRight :size="15" />
              </button>
              <p v-if="!shops.length" class="empty">
                {{ busy ? "正在搜索…" : "附近没有匹配的商铺" }}
              </p>
            </section>
            <section class="panel">
              <div class="section-title">
                <h2>商铺详情</h2>
                <MapPin :size="16" />
              </div>
              <div v-if="detail" class="body">
                <h3>{{ detail.name }}</h3>
                <p>{{ detail.address }} · {{ detail.open_hours }}</p>
                <h4>到店预约</h4>
                <label
                  >人数<input
                    v-model.number="party"
                    type="number"
                    min="1" /></label
                ><button v-for="s in slots" @click="act(() => reserve(s.id))">
                  {{ new Date(s.startsAt).toLocaleString() }} · 余
                  {{ s.remaining }} 位</button
                ><small v-if="!slots.length">暂无可预约时段</small>
              </div>
              <div v-else class="empty">
                <MapPin />
                <p>选择商铺查看详情和可预约时段</p>
              </div>
            </section>
          </div></template
        >
        <template v-if="tab === 1"
          ><div class="cards">
            <section v-for="v in vouchers" class="panel body">
              <Ticket />
              <h3>{{ v.title }}</h3>
              <p>
                {{ v.type ? "限时秒杀" : "普通优惠券" }} · 面值 ¥{{
                  v.actual_value / 100
                }}
              </p>
              <button class="primary" @click="act(() => buy(v))">
                {{ v.type ? "¥" + v.pay_value / 100 + " 抢购" : "领取优惠券" }}
              </button>
            </section>
          </div>
          <section class="panel">
            <div class="section-title"><h2>我的订单</h2></div>
            <p class="body" v-if="pending">排队中 · {{ pending }}</p>
            <div v-for="o in orders" class="row">
              <span class="grow"
                ><strong>{{ o.title }}</strong
                ><small>{{ o.id }}</small></span
              >{{ statuses[o.status]
              }}<button
                v-if="o.status === 1 || o.status === 2"
                @click="act(() => pay(o.id))"
              >
                {{ o.status === 1 ? "模拟支付" : "查看核销码" }}
              </button>
            </div>
            <p v-if="!orders.length && !pending" class="empty">暂无订单</p>
            <p v-if="receipt" class="notice">核销码 · {{ receipt }}</p>
          </section>
          <form
            class="panel body"
            @submit.prevent="
              act(async () => {
                if (!auth()) return;
                await call('/orders/' + redeemID + '/redeem', {
                  code: credential,
                });
                message = '核销成功';
                await refreshOrders();
              })
            "
          >
            <h2><ScanLine :size="17" />门店核销</h2>
            <input v-model="redeemID" required placeholder="订单号" /><input
              v-model="credential"
              required
              placeholder="买家核销码"
            /><button>核销</button>
          </form></template
        >
        <template v-if="tab === 2"
          ><div class="columns">
            <section class="panel">
              <div class="section-title">
                <h2>热门笔记</h2>
                <Heart :size="16" />
              </div>
              <article v-for="n in notes" class="body">
                <small>{{ n.author }}</small>
                <h3>{{ n.title }}</h3>
                <p>{{ n.content }}</p>
                <button
                  @click="
                    act(async () => {
                      if (auth())
                        n.liked = await call('/notes/' + n.id + '/like', {});
                    })
                  "
                >
                  <Heart :size="15" />{{ n.liked }}
                </button>
              </article>
              <p v-if="!notes.length" class="empty">
                暂无笔记，写下第一篇探店体验。
              </p>
            </section>
            <form class="panel body" @submit.prevent="act(publish)">
              <h2>发布笔记</h2>
              <label
                >商铺编号<input
                  v-model.number="shopID"
                  type="number"
                  min="1"
                  required /></label
              ><input
                v-model="title"
                maxlength="255"
                required
                placeholder="标题"
              /><textarea
                v-model="content"
                maxlength="2048"
                required
                placeholder="分享探店体验"
              /><button class="primary">发布</button>
            </form>
          </div></template
        >
        <template v-if="tab === 3"
          ><form
            v-if="!token"
            class="panel body compact"
            @submit.prevent="act(login)"
          >
            <h2>验证码登录</h2>
            <label
              >邮箱或手机号<input
                v-model.trim="account"
                required
                autocomplete="username" /></label
            ><label
              >验证码<input
                v-model="code"
                required
                inputmode="numeric"
                autocomplete="one-time-code"
            /></label>
            <div class="actions">
              <button type="button" @click="act(send)">获取验证码</button
              ><button class="primary">登录</button>
            </div>
          </form>
          <section v-else class="panel body compact">
            <h2><CalendarDays :size="18" />本月签到</h2>
            <div class="number">
              {{ reward?.days || 0 }}
              <small>/ {{ reward?.daysRequired || "—" }} 天</small>
            </div>
            <p>
              连续签到 {{ reward?.streak || 0 }} 天 ·
              {{ reward?.claimed ? "奖励已发放" : "满勤自动发放优惠券" }}
            </p>
            <button
              class="primary"
              :disabled="reward?.signed"
              @click="
                act(async () => {
                  reward = await call('/user/sign', {});
                })
              "
            >
              <Check :size="16" />{{
                reward?.signed ? "今日已签到" : "今日签到"
              }}</button
            ><button @click="act(logout)">退出登录</button>
          </section>
          <section v-if="wallet.length" class="panel body">
            <h2>我的优惠券</h2>
            <div v-for="v in wallet">{{ v.title }} · {{ v.source }}</div>
          </section>
          <section v-if="bookings.length" class="panel body">
            <h2>我的预约</h2>
            <div v-for="b in bookings">
              #{{ b.id }} · {{ b.partySize }} 位
              <button
                :disabled="b.cancelled"
                @click="
                  act(async () => {
                    await call('/bookings/' + b.id + '/cancel', {});
                    b.cancelled = true;
                  })
                "
              >
                {{ b.cancelled ? "已取消" : "取消预约" }}
              </button>
            </div>
          </section></template
        >
      </div>
    </main>
  </div>
</template>
