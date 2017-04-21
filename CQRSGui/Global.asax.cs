using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Routing;
using NEventStore;
using NEventStore.Dispatcher;
using NEventStore.Persistence.Sql.SqlDialects;
using SimpleCQRS;

namespace CQRSGui
{
    // Note: For instructions on enabling IIS6 or IIS7 classic mode, 
    // visit http://go.microsoft.com/?LinkId=9394801

    public class MvcApplication : System.Web.HttpApplication
    {
        public static void RegisterRoutes(RouteCollection routes)
        {
            routes.IgnoreRoute("{resource}.axd/{*pathInfo}");

            routes.MapRoute(
                "Default", // Route name
                "{controller}/{action}/{id}", // URL with parameters
                new { controller = "Home", action = "Index", id = UrlParameter.Optional } // Parameter defaults
            );

        }

        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();

            RegisterRoutes(RouteTable.Routes);

            var bus = new FakeBus();
            var store = WireupEventStore();
            var storage = new EventStore(bus, store);
            var rep = new Repository<InventoryItem>(storage);
            var commands = new InventoryCommandHandlers(rep);
            bus.RegisterHandler<CheckInItemsToInventory>(commands.Handle);
            bus.RegisterHandler<CreateInventoryItem>(commands.Handle);
            bus.RegisterHandler<DeactivateInventoryItem>(commands.Handle);
            bus.RegisterHandler<RemoveItemsFromInventory>(commands.Handle);
            bus.RegisterHandler<RenameInventoryItem>(commands.Handle);
            var detail = new InventoryItemDetailView();
            bus.RegisterHandler<InventoryItemCreated>(detail.Handle);
            bus.RegisterHandler<InventoryItemDeactivated>(detail.Handle);
            bus.RegisterHandler<InventoryItemRenamed>(detail.Handle);
            bus.RegisterHandler<ItemsCheckedInToInventory>(detail.Handle);
            bus.RegisterHandler<ItemsRemovedFromInventory>(detail.Handle);
            var list = new InventoryListView();
            bus.RegisterHandler<InventoryItemCreated>(list.Handle);
            bus.RegisterHandler<InventoryItemRenamed>(list.Handle);
            bus.RegisterHandler<InventoryItemDeactivated>(list.Handle);
            ServiceLocator.Bus = bus;

            RestoreReadModel(store);
        }

        private static IStoreEvents WireupEventStore()
        {
            return Wireup.Init()
                         //.LogToOutputWindow()
                         //.UsingInMemoryPersistence()
                         .UsingSqlPersistence("Default") // Connection string is in app.config
                         .WithDialect(new MsSqlDialect())
                         //.EnlistInAmbientTransaction() // two-phase commit
                         .InitializeStorageEngine()
                         //.TrackPerformanceInstance("example")
                         .UsingJsonSerialization()
                         //.Compress()
                         //.EncryptWith(EncryptionKey)
                         //.HookIntoPipelineUsing(new[] { new AuthorizationPipelineHook() })
                         //.DispatchTo(new DelegateMessageDispatcher(DispatchCommit))
                         .Build();
        }

        private static void RestoreReadModel(IStoreEvents store)
        {
            var bus = ServiceLocator.Bus;
            var commits = store.Advanced.GetFrom();
            foreach (var commit in commits)
            {
                foreach (var ev in commit.Events)
                {
                    bus.Publish(ev.Body as Event);
                }
            }
        }
    }
}