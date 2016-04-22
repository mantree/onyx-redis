(ns onyx.plugin.redis-loop-job-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [pipe <!! put! close!]]
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

(defn my-inc
  [segment]
  (update segment :number inc))

(defn close-redis-loop
  [event {:keys [redis/key]}]
  (let [sentinel "done"
        r (wcar (redis-conn) (car/get key))]
    (when-not (= sentinel r)
      (wcar (redis-conn)
            (car/set key sentinel))))
  {})

(def loop-done-calls
  {:lifecycle/after-batch close-redis-loop})

(defn enough?
  [_ _ {:keys [number]} _]
  (> number 9))

(defn build-job [redis-spec batch-size batch-timeout]
  (let [redis-uri (get-in redis-spec [:spec :uri])
        test-key (str (java.util.UUID/randomUUID))
        batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job {:workflow [[:in :write-state]
                             [:read-state :inc]
                             [:inc :write-state]
                             [:inc :out]]
                  :catalog [{:onyx/name :inc
                             :onyx/fn   :onyx.plugin.redis-loop-job-test/my-inc
                             :onyx/type :function
                             :onyx/batch-size batch-size}]
                  :lifecycles [{:lifecycle/task :out
                                :lifecycle/calls :onyx.plugin.redis-loop-job-test/loop-done-calls
                                :redis/key test-key
                                :lifecycle/doc "Once we exit the loop, tell indicate we're done by posting sentinel to Redis"}]
                  :flow-conditions [{:flow/from :inc
                                     :flow/to [:write-state]
                                     :flow/predicate [:not :onyx.plugin.redis-loop-job-test/enough?]}
                                    {:flow/from :inc
                                     :flow/to [:out]
                                     :flow/predicate :onyx.plugin.redis-loop-job-test/enough?}]
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (core-async/input :in batch-settings))
        (add-task (redis/writer :write-state redis-uri test-key batch-settings))
        (add-task (redis/reader :read-state redis-uri test-key batch-settings))
        (add-task (core-async/output :out batch-settings)))))

(deftest redis-loop-test
  (let [{:keys [env-config
                peer-config]} @config
        redis-spec (redis-conn)
        job (build-job redis-spec 1 1000)
        {:keys [out in]} (get-core-async-channels job)
        test-state {:test "blah" :number 0}]
    (with-test-env [test-env [6 env-config peer-config]]
      (pipe (spool [test-state :done]) in)
      (onyx.test-helper/validate-enough-peers! test-env job)
      (let [job-id (:job-id (onyx.api/submit-job peer-config job))
            got (<!! out)]
        (prn "GOT: " got)
        (testing "redis :sadd and :smembers are correctly distributed"
          (is (= got
                 {:test "blah" :number 10})))
        (prn "Waiting for job to complete...")
        (onyx.api/await-job-completion peer-config job-id)
        (prn "Test completed.")))))
