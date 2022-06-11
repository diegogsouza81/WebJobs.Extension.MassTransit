using System;
using Microsoft.Extensions.Options;
using MassTransit.Clients;

namespace Microsoft.Azure.WebJobs.Extensions.MassTransit
{
    internal class RabbitMQClientBuilder : IConverter<RabbitMQAttribute, IModel>
    {
        private readonly RabbitMQExtensionConfigProvider configProvider;
        private readonly IOptions<RabbitMQOptions> options;

        public RabbitMQClientBuilder(RabbitMQExtensionConfigProvider configProvider, IOptions<RabbitMQOptions> options)
        {
            this.configProvider = configProvider;
            this.options = options;
        }

        public IModel Convert(RabbitMQAttribute attribute)
        {
            return this.CreateModelFromAttribute(attribute);
        }

        private IModel CreateModelFromAttribute(RabbitMQAttribute attribute)
        {
            if (attribute == null)
            {
                throw new ArgumentNullException(nameof(attribute));
            }

            string resolvedConnectionString = Utility.FirstOrDefault(attribute.ConnectionStringSetting, this.options.Value.ConnectionString);
            bool resolvedDisableCertificateValidation = Utility.FirstOrDefault(attribute.DisableCertificateValidation, this.options.Value.DisableCertificateValidation);

            IRabbitMQService service = this.configProvider.GetService(resolvedConnectionString, resolvedDisableCertificateValidation);

            return service.Model;
        }
    }
}
