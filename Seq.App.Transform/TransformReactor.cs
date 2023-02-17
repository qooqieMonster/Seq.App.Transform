using Jurassic;
using Jurassic.Library;
using Seq.Apps;
using Seq.Apps.LogEvents;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Timers;

namespace Seq.App.Transform
{
    [SeqApp("Transform",
        Description = "Collects events and allows a javascript to transform them into different events written back to the log.")]
    public class TransformReactor : SeqApp, ISubscribeTo<LogEventData>
    {
        private Queue<LogEventData> _window;
        private Timer _timer;
        private readonly ConcurrentDictionary<string, bool> _incidents = new ConcurrentDictionary<string, bool>();

        [SeqAppSetting(
            DisplayName = "Window (seconds)",
            HelpText = "The number of seconds within which the events will be collected and available through aggregate functions. Set to 0 to only collect the last event.")]
        public int WindowSeconds { get; set; }

        [SeqAppSetting(
            DisplayName = "Interval (seconds)",
            IsOptional = true,
            HelpText = "How often the script will run. Set to 0 to run on each received event.")]
        public int IntervalSeconds { get; set; }

        [SeqAppSetting(
            DisplayName = "Script (Javascript)",
            IsOptional = false,
            InputType = SettingInputType.LongText,
            HelpText = "The script for transforming the events. Documentation can be found here: https://github.com/stayhard/Seq.App.Transform")]
        public string Script { get; set; }

        protected override void OnAttached()
        {
            base.OnAttached();

            lock (this)
            {
                _window = new Queue<LogEventData>();
            }

            if (IntervalSeconds <= 0) return;
            _timer = new Timer
            {
                AutoReset = false,
                Enabled = false,
                Interval = IntervalSeconds * 1000,
                Site = null,
                SynchronizingObject = null
            };
            _timer.Elapsed += (s, e) =>
            {
                lock (this)
                {
                    Transform();
                }
            };
            _timer.Start();
        }
        
        private static double? ToDouble(object v)
        {
            switch (v)
            {
                case NumberInstance instance:
                    return instance.Value;
                case double d:
                    return d;
                case decimal @decimal:
                    return (double)@decimal;
                case long l:
                    return l;
                default:
                    return null;
            }
        }

        /// <summary>
        /// Jurassic can't handle decimals for some reason, so we cast them all to doubles.
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        private static object FixDecimal(object v)
        {
            if (v is decimal @decimal)
                return (double)@decimal;
            return v;
        }

        private void Transform()
        {
            try
            {
                List<LogEventData> window = WindowSeconds > 0 ? _window.Where(r => r.LocalTimestamp >= DateTime.Now.AddSeconds(-WindowSeconds)).ToList() : new List<LogEventData>();

                if (WindowSeconds <= 0 && _window.Any())
                {
                    window.Add(_window.Last());
                }

                ScriptEngine engine = new ScriptEngine { EnableExposedClrTypes = true };

                Dictionary<string, ArrayInstance> properties = new Dictionary<string, ArrayInstance>
                {
                    { "$Id", engine.Array.Construct() },
                    { "$Level", engine.Array.Construct() },
                    { "$Timestamp", engine.Array.Construct() },
                    { "$Message", engine.Array.Construct() },
                };

                foreach (LogEventData e in window)
                {
                    properties["$Id"].Push(e.Id);
                    properties["$Level"].Push(e.Level);
                    properties["$Timestamp"].Push(e.LocalTimestamp);
                    properties["$Message"].Push(e.RenderedMessage);

                    foreach (KeyValuePair<string, object> p in e.Properties)
                    {
                        if (!properties.ContainsKey(p.Key))
                        {
                            properties.Add(p.Key, engine.Array.Construct());
                        }
                        properties[p.Key].Push(FixDecimal(p.Value));
                    }
                }
                
                engine.SetGlobalValue("all", new Aggregator(engine, properties, r => r));
                engine.SetGlobalValue("count", new Aggregator(engine, properties, r => r.Length));
                engine.SetGlobalValue("first", new Aggregator(engine, properties, r => r.ElementValues.FirstOrDefault()));
                engine.SetGlobalValue("last", new Aggregator(engine, properties, r => r.ElementValues.LastOrDefault()));
                engine.SetGlobalValue("max", new Aggregator(engine, properties, r =>
                {
                    double? result = null;
                    foreach (object v in r.ElementValues)
                    {
                        double? d = ToDouble(v);
                        if (d != null && result == null || result < d)
                        {
                            result = d;
                        }
                    }
                    if (result != null)
                        return result;
                    return "-";
                }));

                engine.SetGlobalValue("min", new Aggregator(engine, properties, r =>
                {
                    double? result = null;
                    foreach (object v in r.ElementValues)
                    {
                        double? d = ToDouble(v);
                        if (d != null && result == null || d < result)
                        {
                            result = d;
                        }
                    }
                    if (result != null)
                        return (double)result;
                    return "-";
                }));

                engine.SetGlobalValue("mean", new Aggregator(engine, properties, r =>
                {
                    double sum = 0;
                    int count = 0;
                    foreach (object v in r.ElementValues)
                    {
                        double? d = ToDouble(v);
                        if (d == null) continue;

                        sum += (double)d;
                        count += 1;
                    }

                    return sum / count;
                }));

                engine.SetGlobalValue("sum", new Aggregator(engine, properties, r =>
                {
                    return r.ElementValues.Select(ToDouble).Where(d => d != null).Sum(d => (double)d);
                }));

                void Verbose(StringInstance a, object b) => GetLoggerFor(b).Verbose(a.Value);
                void Debug(StringInstance a, object b) => GetLoggerFor(b).Debug(a.Value);
                void Information(StringInstance a, object b) => GetLoggerFor(b).Information(a.Value);
                void Warning(StringInstance a, object b) => GetLoggerFor(b).Warning(a.Value);
                void Error(StringInstance a, object b) => GetLoggerFor(b).Error(a.Value);
                void Fatal(StringInstance a, object b) => GetLoggerFor(b).Fatal(a.Value);

                engine.SetGlobalFunction("logTrace", (Action<StringInstance, object>)Verbose);
                engine.SetGlobalFunction("logVerbose", (Action<StringInstance, object>)Verbose);
                engine.SetGlobalFunction("logDebug", (Action<StringInstance, object>)Debug);
                engine.SetGlobalFunction("logInfo", (Action<StringInstance, object>)Information);
                engine.SetGlobalFunction("logInformation", (Action<StringInstance, object>)Information);
                engine.SetGlobalFunction("logWarn", (Action<StringInstance, object>)Warning);
                engine.SetGlobalFunction("logWarning", (Action<StringInstance, object>)Warning);
                engine.SetGlobalFunction("logError", (Action<StringInstance, object>)Error);
                engine.SetGlobalFunction("logFatal", (Action<StringInstance, object>)Fatal);

                engine.SetGlobalFunction("openIncident", new Action<StringInstance>(name =>
                {
                    if (_incidents.TryAdd(name.Value, true) || _incidents.TryUpdate(name.Value, true, false))
                    {
                        Log.ForContext("IncidentState", "Open").Error("[ Incident Open ] {IncidentName}", name);
                    }
                }));
                engine.SetGlobalFunction("closeIncident", new Action<StringInstance>(name =>
                {
                    if (!_incidents.ContainsKey(name.Value))
                    {
                        _incidents.TryAdd(name.Value, false);
                    }
                    if (_incidents.TryUpdate(name.Value, false, true))
                    {
                        Log.ForContext("IncidentState", "Closed").Information("[ Incident Closed ] {IncidentName}", name);
                    }
                }));
                
                engine.Evaluate(Script);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to transform event");
            }
        }

        private class Aggregator : ObjectInstance
        {
            private readonly IDictionary<string, ArrayInstance> _properties;
            private readonly Func<ArrayInstance, object> _func;

            public Aggregator(ScriptEngine engine, IDictionary<string, ArrayInstance> properties, Func<ArrayInstance, object> func) : base(engine)
            {
                _properties = properties;
                _func = func;
            }

            protected override object GetMissingPropertyValue(string propertyName)
            {
                if (!_properties.ContainsKey(propertyName))
                {
                    _properties.Add(propertyName, Engine.Array.Construct());
                }
                return _func(_properties[propertyName]);
            }
        }

        private static object ToClrType(object v)
        {
            switch (v)
            {
                case ArrayInstance instance:
                    return instance.ElementValues.Select(ToClrType).ToList();
                case ClrInstanceWrapper wrapper:
                    return wrapper.WrappedInstance;
                case ObjectInstance instance:
                    return instance.Properties.ToDictionary(r => r.Name, r => ToClrType(r.Value));
                default:
                    return v;
            }
        }

        private ILogger GetLoggerFor(object properties)
        {
            ILogger l = Log;
            if (!(properties is ObjectInstance instance)) return l;

            return instance.Properties.Aggregate(l, (current, prop) => current.ForContext(prop.Name, ToClrType(prop.Value), true));
        }

        public void On(Event<LogEventData> evt)
        {
            lock (this)
            {
                if (WindowSeconds > 0)
                {
                    _window.Enqueue(evt.Data);

                    while (_window.Count > 0 && _window.Peek().LocalTimestamp < DateTime.Now.AddSeconds(-WindowSeconds))
                    {
                        _window.Dequeue();
                    }
                }
                else
                {
                    _window.Clear();
                    _window.Enqueue(evt.Data);
                }

                if (IntervalSeconds <= 0)
                {
                    Transform();
                }
            }
        }
    }
}