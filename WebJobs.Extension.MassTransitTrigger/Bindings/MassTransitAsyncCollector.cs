﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MassTransit.Clients;

namespace Microsoft.Azure.WebJobs.Extensions.MassTransit
{
    internal class MassTransitAsyncCollector : IAsyncCollector<ReadOnlyMemory<byte>>
    {
        private readonly RabbitMQContext context;
        private readonly ILogger logger;

        public MassTransitAsyncCollector(RabbitMQContext context, ILogger logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _ = context ?? throw new ArgumentNullException(nameof(context));
            _ = context.Service ?? throw new ArgumentException("Value cannot be null. Parameter name: context.Service");
            this.context = context;
        }

        public Task AddAsync(ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default)
        {
            this.logger.LogDebug("Adding message to batch for publishing...");

            lock (this.context.Service.PublishBatchLock)
            {
                this.context.Service.BasicPublishBatch.Add(exchange: string.Empty, routingKey: this.context.ResolvedAttribute.QueueName, mandatory: false, properties: null, body: message);
            }

            return Task.CompletedTask;
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            return this.PublishAsync();
        }

        internal Task PublishAsync()
        {
            this.logger.LogDebug("Publishing messages to queue.");

            lock (this.context.Service.PublishBatchLock)
            {
                this.context.Service.BasicPublishBatch.Publish();
                this.context.Service.ResetPublishBatch();
            }

            return Task.CompletedTask;
        }
    }
}
