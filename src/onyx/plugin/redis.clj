(ns onyx.plugin.redis
  (:require [clojure.core.async :as async :refer [timeout]]
            [onyx.peer function
             [pipeline-extensions :as p-ext]]
            [onyx.static
             [default-vals :refer [arg-or-default]]
             [uuid :refer [random-uuid]]]
            [onyx.types :as t]
            [taoensso.carmine :as car :refer [wcar]]))

(defrecord Ops [sadd lpush zadd set lpop spop rpop pfcount pfadd publish get])

(def operations
  (->Ops car/sadd car/lpush car/zadd car/set car/lpop car/spop car/rpop car/pfcount car/pfadd car/publish car/get))

;;;;;;;;;;;;;;;;;;;;
;; Connection lifecycle code

(defn inject-conn-spec [{:keys [onyx.core/params onyx.core/task-map] :as event}
                        {:keys [redis/param? redis/uri redis/read-timeout-ms] :as lifecycle}]
  (when-not (or uri (:redis/uri task-map))
      (throw (ex-info ":redis/uri must be supplied to output task." lifecycle)))
  (let [conn {:spec {:uri (or (:redis/uri task-map) uri)
                     :read-timeout-ms (or read-timeout-ms 4000)}}]
    {:onyx.core/params (if (or (:redis/param? task-map) param?)
                         (conj params conn)
                         params)
     :redis/conn conn}))

(def reader-conn-spec
  {:lifecycle/before-task-start inject-conn-spec})

;;;;;;;;;;;;;;;;;;;;;
;; Output plugin code

(defrecord RedisWriter [conn k]
  p-ext/Pipeline
  (read-batch [_ event]
    (onyx.peer.function/read-batch event))

  (write-batch [_ {:keys [onyx.core/results] :as everything}]
    (when-let [msg (:message (first (mapcat :leaves (:tree results))))]
      (prn "Writing msg" msg)
      (wcar conn (car/set k msg)))
    {})
  (seal-resource [_ _]
    {}))

(defn writer [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        uri (:redis/uri catalog-entry)
        k (:redis/key catalog-entry)
        _ (when-not uri
            (throw (ex-info ":redis/uri must be supplied to output task." catalog-entry)))
        conn          {:spec {:uri uri
                              :read-timeout-ms (or (:redis/read-timeout-ms catalog-entry)
                                                   4000)}}]
    (->RedisWriter conn k)))

;;;;;;;;;;;;;;;;;;;;;
;; Input plugin code

(defn inject-pending-state [event lifecycle]
  (let [pipeline (:onyx.core/pipeline event)
        task     (:onyx.core/task-map event)]
    (when (> (:onyx/max-peers task) 1)
      (throw (Exception. "Onyx-Redis can not run with :onyx/max-peers greater than 1")))
    {:redis/conn             (:conn pipeline)
     :redis/drained?         (:drained? pipeline)
     :redis/pending-messages (:pending-messages pipeline)}))

(defn done? [message]
  (= "done" (:message message)))

(defn take-from-redis
  "conn: Carmine map for connecting to redis
  key: The name of a redis key for looking up the relevant values
  batch-size: The maximum size of a returned batch
  timeout: stop processing new collections after `timeout`/ms''

  In order to stay consistent in Redis list consumption, this function will
  finish processing the batch it's currently on before returning. This means that
  if your batch sizes are large and steps are small, it is possible to block for
  and extended ammount of time. Returns nil if the list is exausted."
  [conn k]
  (wcar conn (car/get k)))

(defrecord RedisReader [conn k pending-state drained? current-state]
  p-ext/Pipeline
  p-ext/PipelineInput
  (write-batch [this event]
    (onyx.peer.function/write-batch event))

  (read-batch [_ event]
    (if @pending-state
      (do
        (if (done? @pending-state)
          (reset! drained? true)
          ;; Allow unset to reduce the chance of draining race conditions
          ;; I believe these are still possible in this plugin thanks to the
          ;; sentinel handling
          (reset! drained? false))
        {:onyx.core/batch [@pending-state]})
      (let [raw-state (take-from-redis conn k)]
        (when-not (= raw-state @current-state)
          (prn "Swapping state: " raw-state @current-state)
          (reset! current-state raw-state)
          (let [state (t/input (random-uuid)
                               raw-state)
                is-done (= "done" raw-state)]
            (reset! drained? is-done)
            (if is-done
              {:onyx.core/batch [(t/input (random-uuid) :done)]}
              (do
                (reset! pending-state state)
                {:onyx.core/batch [state]})))))))

  (ack-segment
      [_ _ _]
    (reset! pending-state nil))

  (retry-segment
      [_ _ _]
    (reset! pending-state nil))

  (pending?
      [_ _ _]
    @pending-state)

  (drained?
      [_ event]
    @drained?)

  (seal-resource
      [_ event]
                                        ;destory key in redis
    ))

(defn reader [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        pending-state (atom nil)
        drained?      (atom false)
        read-timeout  (or (:redis/read-timeout-ms catalog-entry) 4000)
        k (:redis/key catalog-entry)
        uri (:redis/uri catalog-entry)
        _ (when-not uri
            (throw (ex-info ":redis/uri must be supplied to output task." catalog-entry)))
        conn             {:pool nil
                          :spec {:uri uri
                                 :read-timeout-ms read-timeout}}]
    (->RedisReader conn k pending-state
                   drained? (atom nil))))

(def reader-state-calls
  {:lifecycle/before-task-start inject-pending-state})
