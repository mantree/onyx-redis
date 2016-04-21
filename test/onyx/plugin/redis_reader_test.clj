(ns onyx.plugin.redis-reader-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [pipe <!!]]
            [clojure.core.async.lab :refer [spool]]
            [clojure.test :refer [deftest is use-fixtures testing]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin
             [redis]
             [core-async :refer [get-core-async-channels]]]
            [onyx.tasks
             [core-async :as core-async]
             [redis :as redis]]
            [taoensso.carmine :as car :refer [wcar]]))


(def config (atom {}))

(defn redis-conn []
  {:spec {:uri (get-in @config [:redis-config :redis/uri])}})

(defn load-config [test-fn]
  (reset! config (read-config (clojure.java.io/resource "config.edn") {:profile :test}))
  (test-fn))

(defn flush-redis [test-fn]
  (wcar (redis-conn)
        (car/flushall)
        (car/flushdb))
  (test-fn))

(use-fixtures :once load-config)
(use-fixtures :each flush-redis)

(def test-key "test-key")

(defn build-job [redis-spec batch-size batch-timeout]
  (let [redis-uri (get-in redis-spec [:spec :uri])
        batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:in :out]]
                         :catalog [{:onyx/name :inc
                                    :onyx/fn ::my-inc
                                    :onyx/type :function
                                    :onyx/batch-size batch-size}]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (redis/reader :in redis-uri test-key :get batch-settings))
        (add-task (core-async/output :out batch-settings)))))

(defn put-state
  [s]
  (wcar (redis-conn)
        (car/set test-key
                 s)))

(deftest redis-reader-test
  (let [{:keys [env-config
                peer-config]} @config
        redis-spec (redis-conn)
        job (build-job redis-spec 1 1000)
        {:keys [out]} (get-core-async-channels job)
        test-state {:test "blah"}]
    (with-test-env [test-env [2 env-config peer-config]]
      (put-state test-state)
      (onyx.test-helper/validate-enough-peers! test-env job)
      (->> (:job-id (onyx.api/submit-job peer-config job))
           (onyx.api/await-job-completion peer-config))
      (prn "Awaiting")
      (let [got (<!! out)]
        (prn "GOT: " got)
        (testing "redis :sadd and :smembers are correctly distributed"
          (is (= got
                 test-state)))))))
