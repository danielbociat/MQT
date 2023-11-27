using MQT.BackgroundTask;
using MQT.Statistics;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

//builder.Services.AddHostedService<ConsumeKafka>();
builder.Services.AddSingleton<Stats>();

var app = builder.Build();

var tokenSource = new CancellationTokenSource();
var logger = builder.Services.BuildServiceProvider().GetRequiredService<ILogger<ConsumeKafka>>();
new Thread(async => new ConsumeKafka(logger).StartAsync(tokenSource.Token)).Start();


// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
