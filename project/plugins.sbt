resolvers += Resolver.typesafeRepo("releases")

resolvers += Classpaths.typesafeReleases

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "2.0.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")
