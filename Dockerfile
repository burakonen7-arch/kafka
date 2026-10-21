FROM mcr.microsoft.com/dotnet/sdk:10.0.402 AS build
WORKDIR /source
COPY . .
ARG SERVICE
RUN dotnet publish src/${SERVICE}/${SERVICE}.Api -c Release -o /app --no-self-contained
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ARG SERVICE
ENV SERVICE_NAME=${SERVICE}
ENV ASPNETCORE_HTTP_PORTS=8080
USER $APP_UID
ENTRYPOINT ["sh", "-c", "exec dotnet ${SERVICE_NAME}.Api.dll"]
